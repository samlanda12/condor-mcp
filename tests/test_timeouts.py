"""Calls to a daemon that does not answer: they run in the worker process, give up at
CONDOR_MCP_TIMEOUT with a clear error, and a write that times out says its outcome is
unknown. The silent schedd is a socket that accepts connections and never replies, which
Schedd.history and Schedd.submit would otherwise wait on forever."""

from __future__ import annotations

import re
import signal
import socket
import threading
import time

import pytest
from fastmcp.exceptions import ToolError

from conftest import call

pytestmark = pytest.mark.anyio
TIMEOUT = 2


@pytest.fixture
def silent_ad():
    """The local schedd's ad, pointed at a socket that accepts connections and never replies."""
    import classad2 as classad
    import htcondor2 as htcondor

    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(16)
    held: list[socket.socket] = []

    def accept() -> None:
        while True:
            try:
                held.append(server.accept()[0])
            except OSError:  # closed at teardown
                return

    threading.Thread(target=accept, daemon=True).start()
    ad = classad.ClassAd()
    ad.update(htcondor.Collector().locate(htcondor.DaemonType.Schedd))
    for attr in ("MyAddress", "ScheddIpAddr"):
        ad[attr] = f"<127.0.0.1:{server.getsockname()[1]}>"
    try:
        yield ad
    finally:
        server.close()
        for conn in held:
            conn.close()


def _use_schedds(monkeypatch, schedds):
    """Point the server at these schedds, by name, with a short timeout."""
    from condor_mcp import pool

    update = {"timeout": TIMEOUT, "schedds": list(schedds)}
    monkeypatch.setattr(pool, "settings", pool.settings.model_copy(update=update))
    monkeypatch.setattr(pool, "locate", lambda name: schedds[name])


@pytest.fixture
def silent_schedd(monkeypatch, silent_ad):
    """One schedd, named "silent", that never answers."""
    import htcondor2 as htcondor

    _use_schedds(monkeypatch, {"silent": htcondor.Schedd(silent_ad)})


def test_worker_gives_up_and_recovers():
    from condor_mcp import worker

    start = time.monotonic()
    with pytest.raises(worker.NoAnswer, match="no answer within 0.5 s"):
        worker.run(0.5, time.sleep, 30)
    assert time.monotonic() - start < 5
    assert worker.run(10, time.monotonic) > 0  # a fresh child took over


def test_hung_call_does_not_hold_up_another():
    from condor_mcp import worker

    errors: list[Exception] = []

    def hang() -> None:
        try:
            worker.run(3, time.sleep, 30)
        except worker.NoAnswer as exc:
            errors.append(exc)

    hung = threading.Thread(target=hang)
    hung.start()
    time.sleep(0.5)  # the hung call is under way
    start = time.monotonic()
    assert worker.run(3, time.monotonic) > 0
    assert time.monotonic() - start < 1
    hung.join()
    assert len(errors) == 1


def test_child_ends_itself_after_its_deadline():
    """A server that exits mid-call never kills the child, so the child's own alarm must."""
    from condor_mcp import worker

    proc, conn = worker._start()
    try:
        conn.send((1, time.sleep, (30,)))
        assert proc.wait(1 + worker.CHILD_GRACE + 5) == -signal.SIGALRM
    finally:
        worker._stop((proc, conn))


def test_crashed_child_is_not_blamed_on_the_timeout():
    import os

    from condor_mcp import pool

    with pytest.raises(pool.NoAnswer, match=r"exited before it answered \(status 3\)$"):
        pool.call(os._exit, 3)


def test_timeout_must_be_finite():
    from pydantic import ValidationError

    from condor_mcp import pool

    with pytest.raises(ValidationError):
        pool.Settings(timeout=float("inf"))


def test_worker_reraises_htcondor_errors():
    """HTCondorException does not pickle, so the worker sends its message and the server
    raises a new one that callers catch as before."""
    import htcondor2 as htcondor

    from condor_mcp import worker

    with pytest.raises(htcondor.HTCondorException):
        worker.run(30, worker.collector_query, "127.0.0.1:1", htcondor.AdType.Startd, "True", [])


async def test_history_times_out(silent_schedd):
    start = time.monotonic()
    with pytest.raises(ToolError, match=rf"schedd \(silent\): no answer within {TIMEOUT} s; CONDOR_MCP_TIMEOUT"):
        await call("job_history")
    assert time.monotonic() - start < TIMEOUT + 5


async def test_queue_query_times_out(silent_schedd):
    with pytest.raises(ToolError, match=rf"schedd \(silent\): no answer within {TIMEOUT} s"):
        await call("list_jobs")


async def test_reads_leave_out_a_schedd_that_does_not_answer(monkeypatch, silent_ad, live_jobs):
    """Each tool waits one timeout for the silent schedd, however many reads it makes."""
    import htcondor2 as htcondor

    _use_schedds(monkeypatch, {"real": htcondor.Schedd(), "silent": htcondor.Schedd(silent_ad)})
    no_answer = rf"schedd \(silent\): no answer within {TIMEOUT} s"
    for tool, args in [
        ("list_jobs", {}),
        ("hold_reasons", {}),
        ("job_history", {}),
        ("job_details", {"cluster_id": live_jobs["running"]}),
    ]:
        start = time.monotonic()
        out = await call(tool, **args)
        assert time.monotonic() - start < TIMEOUT + 1.5, tool
        (warning,) = out["warnings"]
        assert re.search(no_answer, warning), (tool, warning)
    assert out["schedd"] == "real" and out["in_queue"] is True


async def test_submit_timeout_says_the_outcome_is_unknown(silent_schedd, submitted_job):
    with pytest.raises(ToolError, match="may or may not have been queued; check with list_jobs"):
        await call("submit_job", submit_text=submitted_job["submit_text"])


async def test_act_timeout_says_the_outcome_is_unknown(silent_schedd, monkeypatch):
    """The lookup before the hold answers; the hold itself does not."""
    import classad2 as classad

    from condor_mcp import pool

    job = classad.ClassAd({"ClusterId": 7, "ProcId": 0, "Owner": pool.settings.user, "JobStatus": 1})
    monkeypatch.setattr(pool, "query_schedd", lambda *args, **kw: [job])
    with pytest.raises(ToolError, match="request to hold job 7.0: no answer .* may or may not have taken effect"):
        await call("hold_job", cluster_id=7, proc_id=0, reason="test")
