"""Run HTCondor calls in a child process, with a deadline.

Some htcondor2 calls (Schedd.history, Schedd.submit) hang forever when a daemon accepts
the connection but never answers, and they hold the GIL while waiting, so one such call
in the server would freeze it. Every call that talks to a daemon therefore runs in a
long-lived child process. If a call misses its deadline, the child is killed (the next
call starts a fresh one) and the caller gets NoAnswer.

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
import time
from multiprocessing.connection import Connection
from typing import Any, Callable, TypeVar

import classad2 as classad
import htcondor2 as htcondor

T = TypeVar("T")


class NoAnswer(Exception):
    """A call did not finish before its deadline, or the child died during it. For a
    write, whether it took effect is unknown."""


# One call at a time: the child serves calls in order, and the lock keeps one caller's
# reply from reaching another.
_lock = threading.Lock()
_child: tuple[subprocess.Popen[bytes], Connection] | None = None


def _serve(conn: Connection) -> None:
    """The child's loop: run each call and send back ("ok", result) or the error."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # a Ctrl-C at the server is the server's to handle
    while True:
        try:
            fn, args = conn.recv()
        except EOFError:  # the server has gone
            return
        try:
            reply: tuple[str, Any] = ("ok", fn(*args))
        except htcondor.HTCondorException as exc:
            reply = ("htcondor", str(exc))
        except Exception as exc:
            reply = ("error", exc)
        try:
            conn.send(reply)
        except Exception as exc:  # a result or exception that does not pickle
            conn.send(("error", RuntimeError(f"{type(exc).__name__}: {exc}")))


def _start() -> tuple[subprocess.Popen[bytes], Connection]:
    """A fresh interpreter running this module, not a fork (the server has threads, and a
    fork could inherit a held lock) and not multiprocessing's spawn (which re-runs the
    server's __main__ in the child, and fails when that is not a file). Its stdout goes
    nowhere, since with the stdio transport the server's stdout carries the protocol."""
    ours, theirs = multiprocessing.Pipe()
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "condor_mcp.worker", str(theirs.fileno())],
        pass_fds=[theirs.fileno()], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, env=env,
    )
    theirs.close()
    return proc, ours


def _stop() -> None:
    global _child
    if _child is not None:
        proc, conn = _child
        _child = None
        proc.kill()
        proc.wait(5)
        conn.close()


def run(timeout: float, fn: Callable[..., T], *args: Any) -> T:
    """fn(*args) in the child, raising NoAnswer after timeout seconds, time spent waiting
    behind another call included. fn must be importable by name, like the functions below."""
    global _child
    deadline = time.monotonic() + timeout
    if not _lock.acquire(timeout=timeout):
        raise NoAnswer(f"no answer within {timeout:g} s (an earlier call was still running)")
    try:
        if _child is None or _child[0].poll() is not None:
            _stop()
            _child = _start()
        _, conn = _child
        try:
            conn.send((fn, args))
            if not conn.poll(max(0.0, deadline - time.monotonic())):
                _stop()
                raise NoAnswer(f"no answer within {timeout:g} s")
            status, value = conn.recv()
        except (EOFError, OSError):  # the child died
            _stop()
            raise NoAnswer("the process making the call exited before it answered") from None
    finally:
        _lock.release()
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


def collector_locate(pool: str | None, daemon_type: htcondor.DaemonType, name: str) -> classad.ClassAd | None:
    return htcondor.Collector(pool).locate(daemon_type, name)


if __name__ == "__main__":
    _serve(Connection(int(sys.argv[1])))
