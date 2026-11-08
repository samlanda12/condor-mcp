"""job_output against the live pool: the finished job's files, a queued job whose
stdout has not come back yet, and the refusals."""

from __future__ import annotations

import pytest
from fastmcp.exceptions import ToolError

from conftest import WORKDIR, call

pytestmark = pytest.mark.anyio


async def test_job_output_stdout(finished_job):
    out = await call("job_output", cluster_id=finished_job)
    assert out["job_id"] == f"{finished_job}.0" and out["which"] == "stdout"
    assert out["status"] == "completed" and out["in_queue"] is False
    assert out["path"] == str(WORKDIR / f"{finished_job}.0.out")
    assert out["text"] == "\n".join(f"line {i}" for i in range(1, 6))
    assert out["truncated"] is False and out["size_bytes"] > 0 and "note" not in out


async def test_job_output_tail(finished_job):
    out = await call("job_output", cluster_id=finished_job, tail_lines=2)
    assert out["text"] == "line 4\nline 5" and out["truncated"] is True


async def test_job_output_stderr(finished_job):
    out = await call("job_output", cluster_id=finished_job, which="stderr")
    assert out["text"] == "to stderr"


async def test_job_output_log(finished_job):
    out = await call("job_output", cluster_id=finished_job, which="log", tail_lines=500)
    assert out["path"] == str(WORKDIR / f"{finished_job}.log")
    assert "Job terminated" in out["text"] and "return value 3" in out["text"]


async def test_job_output_before_it_comes_back(submitted_job):
    """A queued job with file transfer: idle has no stdout yet, nor does running."""
    out = await call("job_output", cluster_id=submitted_job["cluster"])
    assert out["in_queue"] is True and out["text"] == ""
    assert out["note"].startswith("No stdout yet: the job is ")


async def test_job_output_discarded(live_jobs):
    # Submitted without an output command, so HTCondor sends stdout to /dev/null
    out = await call("job_output", cluster_id=live_jobs["running"])
    assert "keeps no stdout" in out["note"] and "path" not in out


async def test_job_output_missing_job():
    with pytest.raises(ToolError, match="neither the queue nor the schedd history"):
        await call("job_output", cluster_id=999999)


async def test_job_output_refuses_another_users_job(finished_job, monkeypatch):
    from condor_mcp import pool, server

    as_other = pool.settings.model_copy(update={"user": "someone-else"})
    monkeypatch.setattr(pool, "settings", as_other)
    monkeypatch.setattr(server, "settings", as_other)
    with pytest.raises(ToolError, match=f"Refusing to read the output of job {finished_job}.0"):
        await call("job_output", cluster_id=finished_job)


async def test_job_output_refuses_an_id_on_two_schedds(finished_job, monkeypatch):
    """The same schedd under two names stands in for two schedds with colliding IDs."""
    import htcondor2 as htcondor

    from condor_mcp import pool

    monkeypatch.setattr(pool, "schedds", lambda: iter([("a", htcondor.Schedd()), ("b", htcondor.Schedd())]))
    with pytest.raises(ToolError, match=rf"Job {finished_job}\.0 exists on more than one schedd \(a, b\)"):
        await call("job_output", cluster_id=finished_job)


def test_tail_of_one_long_line(tmp_path):
    """A last line longer than the cap comes back as its final MAX_OUTPUT_BYTES, marked cut."""
    from condor_mcp.server import MAX_OUTPUT_BYTES, _tail

    big = tmp_path / "big.out"
    big.write_text("x" * (2 * MAX_OUTPUT_BYTES) + "\n")
    text, size, truncated = _tail(big, 10)
    assert text == "x" * (MAX_OUTPUT_BYTES - 1) and size == 2 * MAX_OUTPUT_BYTES + 1 and truncated
