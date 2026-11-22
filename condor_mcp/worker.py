"""Run HTCondor calls in a child process, with a deadline.

Some htcondor2 calls (Schedd.history, Schedd.submit) hang forever when a daemon accepts
the connection but never answers, and they hold the GIL while waiting, so one such call
in the server would freeze it. Every call that talks to a daemon therefore runs in a
child process, one call per child at a time, so a hung call holds up only its own caller.
Children are reused between calls. If a call misses its deadline, its child is killed and
the caller gets NoAnswer. The child also arms an alarm for each call that kills it a little
after the deadline, so it cannot outlive a server that exits mid-call.

Arguments and results must pickle. Collector and Submit objects don't, so the calls below
take the pool string or submit text and build them in the child. HTCondorException
doesn't either, so the server re-raises it from the child's message.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

import classad2 as classad
import htcondor2 as htcondor

# How long after its deadline a child in a call kills itself, if the server has not
CHILD_GRACE = 5
# Idle children kept for reuse; more are stopped once their call ends
MAX_IDLE = 4


class NoAnswer(Exception):
    """A call did not finish before its deadline (timed_out), or its child died during it.
    For a write, whether it took effect is unknown."""

    def __init__(self, message: str, timed_out: bool = False) -> None:
        super().__init__(message)
        self.timed_out = timed_out


Child = tuple[subprocess.Popen[bytes], Connection]
_idle: list[Child] = []
_idle_lock = threading.Lock()


def _serve(conn: Connection) -> None:
    """The child's loop: run each call and send back ("ok", result) or the error."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # a Ctrl-C at the server is the server's to handle
    while True:
        try:
            timeout, fn, args = conn.recv()
        except EOFError:  # the server has gone
            return
        # SIGALRM's default action ends the process, even inside a C call holding the GIL
        signal.setitimer(signal.ITIMER_REAL, timeout + CHILD_GRACE)
        try:
            reply: tuple[str, Any] = ("ok", fn(*args))
        except htcondor.HTCondorException as exc:
            reply = ("htcondor", str(exc))
        except Exception as exc:  # any other error goes back to the server  # noqa: BLE001
            reply = ("error", exc)
        signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            conn.send(reply)
        except Exception as exc:  # a result or exception that does not pickle  # noqa: BLE001
            conn.send(("error", RuntimeError(f"{type(exc).__name__}: {exc}")))


def _start() -> Child:
    """A fresh interpreter running this module, not a fork (the server has threads, and a
    fork could inherit a held lock) and not multiprocessing's spawn (which re-runs the
    server's __main__ in the child, and fails when that is not a file). Its stdout goes
    nowhere, since with the stdio transport the server's stdout carries the protocol."""
    ours, theirs = multiprocessing.Pipe()
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "condor_mcp.worker", str(theirs.fileno())],
            pass_fds=[theirs.fileno()],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            env=env,
        )
    except OSError as exc:  # out of processes, memory or file descriptors
        ours.close()
        raise NoAnswer(f"could not start the process that makes the call: {exc}") from None
    finally:
        theirs.close()
    return proc, ours


def _stop(child: Child) -> None:
    proc, conn = child
    conn.close()
    proc.kill()
    try:
        proc.wait(5)
    except subprocess.TimeoutExpired:  # stuck in the kernel; subprocess reaps it later
        pass


def _take() -> Child:
    with _idle_lock:
        while _idle:
            child = _idle.pop()
            if child[0].poll() is None:
                return child
            child[1].close()
    return _start()


def _give_back(child: Child) -> None:
    with _idle_lock:
        if len(_idle) < MAX_IDLE:
            _idle.append(child)
            return
    _stop(child)


def run[T](timeout: float, fn: Callable[..., T], *args: Any) -> T:
    """fn(*args) in a child, raising NoAnswer after timeout seconds. fn must be importable
    by name, like the functions below."""
    child = _take()
    proc, conn = child
    try:
        conn.send((timeout, fn, args))
        if not conn.poll(timeout):
            raise NoAnswer(f"no answer within {timeout:g} s", timed_out=True)
        status, value = conn.recv()
    except (EOFError, OSError):  # the child died
        _stop(child)
        raise NoAnswer(f"the process making the call exited before it answered (status {proc.returncode})") from None
    except BaseException:  # a timeout, or an argument that does not pickle
        _stop(child)
        raise
    _give_back(child)
    if status == "htcondor":
        raise htcondor.HTCondorException(value)
    if status == "error":
        raise value
    return value  # type: ignore[no-any-return]


# The calls, run in the child


def schedd_query(schedd: htcondor.Schedd, constraint: str, projection: list[str], limit: int) -> list[classad.ClassAd]:
    return schedd.query(constraint=constraint, projection=projection, limit=limit)


def schedd_history(
    schedd: htcondor.Schedd, constraint: str, projection: list[str], match: int, since: str | None
) -> list[classad.ClassAd]:
    return list(schedd.history(constraint, projection, match=match, since=since))


def schedd_act(
    schedd: htcondor.Schedd, action: htcondor.JobAction, constraint: str, reason: tuple[str, int | None]
) -> classad.ClassAd | None:
    return schedd.act(action, constraint, reason=reason)


def schedd_submit(schedd: htcondor.Schedd, text: str, overrides: dict[str, str]) -> htcondor.SubmitResult:
    """Submit text, with overrides on top. Takes text because str(Submit) drops inline queue items."""
    description = htcondor.Submit(text)
    description.update(overrides)
    return schedd.submit(description)


def collector_query(
    pool: str | None, ad_type: htcondor.AdType, constraint: str, projection: list[str]
) -> list[classad.ClassAd]:
    return htcondor.Collector(pool).query(ad_type, constraint=constraint, projection=projection)


def collector_locate(pool: str | None, daemon_type: htcondor.DaemonType, name: str | None) -> classad.ClassAd | None:
    if name is None:  # the pool's only daemon of that type, such as its negotiator
        return htcondor.Collector(pool).locate(daemon_type)
    return htcondor.Collector(pool).locate(daemon_type, name)


def negotiator_priorities(location: classad.ClassAd | None) -> list[classad.ClassAd]:
    """The negotiator's accounting ads, one per submitter and one per accounting group.
    None is the local config's negotiator."""
    return htcondor.Negotiator(location).getPriorities(False)


if __name__ == "__main__":
    _serve(Connection(int(sys.argv[1])))
