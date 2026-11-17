"""Calls to a daemon that does not answer: they run in the worker process, give up at
CONDOR_MCP_TIMEOUT with a clear error, and a write that times out says its outcome is
unknown. The silent schedd is a socket that accepts connections and never replies, which
Schedd.history and Schedd.submit would otherwise wait on forever."""

from __future__ import annotations

import socket
import threading
import time

import pytest
from fastmcp.exceptions import ToolError

from conftest import call

pytestmark = pytest.mark.anyio
TIMEOUT = 2


@pytest.fixture
def silent_schedd(monkeypatch):
    """Point the server at one schedd, named "silent", that never answers, with a short timeout."""
    import classad2 as classad
    import htcondor2 as htcondor

    from condor_mcp import pool

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
    monkeypatch.setattr(pool, "settings", pool.settings.model_copy(update={"timeout": TIMEOUT}))
    monkeypatch.setattr(pool, "schedds", lambda: iter([("silent", htcondor.Schedd(ad))]))
    try:
        yield
    finally:
        server.close()
        for conn in held:
            conn.close()


def test_worker_gives_up_and_recovers():
    from condor_mcp import worker

    start = time.monotonic()
    with pytest.raises(worker.NoAnswer, match="no answer within 0.5 s"):
        worker.run(0.5, time.sleep, 30)
    assert time.monotonic() - start < 5
    assert worker.run(10, time.monotonic) > 0  # a fresh child took over


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
