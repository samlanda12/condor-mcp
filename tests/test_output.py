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


async def test_job_output_empty_before_it_comes_back(fresh_job):
    """condor_submit creates stdout empty at submit; the bindings do not, so make it here."""
    out_file = WORKDIR / f"{fresh_job}.0.out"
    out_file.touch()
    try:
        out = await call("job_output", cluster_id=fresh_job)
    finally:
        out_file.unlink()
    assert out["size_bytes"] == 0 and out["note"].startswith("No stdout yet: the job is ")


def test_no_pending_note_once_the_job_has_ended():
    """A completed job left in the queue is done, so a missing stdout is not on its way."""
    import classad2 as classad

    from condor_mcp.server import _pending_output_note

    ad = classad.ClassAd({"JobStatus": 4, "NumJobStarts": 1, "ShouldTransferFiles": "YES"})
    assert _pending_output_note(ad, "stdout", in_queue=True) is None
    ad["JobStatus"] = 2
    assert _pending_output_note(ad, "stdout", in_queue=True).startswith("No stdout yet: the job is running")


async def test_job_output_relative_path_without_iwd(monkeypatch):
    import classad2 as classad

    from condor_mcp import pool

    ad = classad.ClassAd({"ClusterId": 7, "ProcId": 0, "Owner": pool.settings.user, "JobStatus": 4, "Out": "job.out"})
    monkeypatch.setattr(pool, "find_job", lambda *args: pool.FoundJob("local", ad, False, []))
    out = await call("job_output", cluster_id=7)
    assert "path" not in out and "records no submit directory (Iwd)" in out["note"]


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


def _two_schedds(monkeypatch, queues, histories):
    """Stand in for schedds a and b: queues and histories map a schedd name to its ads
    of job 7.0, and a history read stops where its since expression turns true. A
    history given as an exception raises it, as an unreadable history does."""
    import classad2 as classad

    from condor_mcp import pool

    def history(name, schedd, constraint, projection, limit, since=None):
        if isinstance(histories.get(name), Exception):
            raise histories[name]
        ads = []
        for ad in histories.get(name, []):  # newest first, like the schedd
            if since and classad.ExprTree(since).eval(ad):
                break
            ads.append(ad)
        return ads

    monkeypatch.setattr(pool, "settings", pool.settings.model_copy(update={"schedds": ["a", "b"]}))
    monkeypatch.setattr(pool, "schedds", lambda: iter([("a", None), ("b", None)]))
    monkeypatch.setattr(pool, "query_schedd", lambda name, *args, **kw: queues.get(name, []))
    monkeypatch.setattr(pool, "query_schedd_history", history)


def _job(**attrs):
    import classad2 as classad

    return classad.ClassAd({"ClusterId": 7, "ProcId": 0, **attrs})


def test_find_job_prefers_the_queue_over_an_older_job_in_another_history(monkeypatch):
    from condor_mcp import pool

    queued = _job(JobStatus=1, QDate=1000)
    older = _job(JobStatus=4, QDate=100, EnteredCurrentStatus=500)
    _two_schedds(monkeypatch, {"a": [queued]}, {"b": [older]})
    assert pool.find_job(7, 0, ["JobStatus"]) == ("a", queued, True, [])


def test_find_job_refuses_a_queued_id_another_history_finished_since(monkeypatch):
    """B's 7.0 left the queue after A's 7.0 was submitted, so either could be meant."""
    from condor_mcp import pool

    queued = _job(JobStatus=1, QDate=1000)
    newer = _job(JobStatus=4, QDate=2000, EnteredCurrentStatus=2100)
    _two_schedds(monkeypatch, {"a": [queued]}, {"b": [newer]})
    with pytest.raises(ToolError, match=r"Job 7\.0 exists on more than one schedd \(a, b\)"):
        pool.find_job(7, 0, ["JobStatus"])


def test_find_job_skips_the_queued_schedds_own_history(monkeypatch):
    """A's 7.0 leaves the queue between the queue read and the history read, so A's
    history has it too; that is the same job, not a second one."""
    from condor_mcp import pool

    queued = _job(JobStatus=2, QDate=1000)
    _two_schedds(monkeypatch, {"a": [queued]}, {"a": [_job(JobStatus=4, QDate=1000, EnteredCurrentStatus=1100)]})
    assert pool.find_job(7, 0, ["JobStatus"]) == ("a", queued, True, [])


def test_find_job_returns_the_queued_job_when_another_history_fails(monkeypatch):
    """B's history cannot be read, so a newer 7.0 there cannot be ruled out; the queued
    job still comes back, with a warning naming B."""
    from condor_mcp import pool

    queued = _job(JobStatus=1, QDate=1000)
    down = ToolError("Could not read the job history of the schedd (b): no answer within 60 s.")
    _two_schedds(monkeypatch, {"a": [queued]}, {"b": down})
    schedd, ad, in_queue, (warning,) = pool.find_job(7, 0, ["JobStatus"])
    assert (schedd, ad, in_queue) == ("a", queued, True)
    assert warning.startswith("Could not read the job history of the schedd (b)")
    assert "schedd b could not be checked for a newer job 7.0; this is the one queued on a" in warning


def test_find_job_still_fails_for_a_finished_job_when_a_history_fails(monkeypatch):
    """With no queue holding the job there is nothing safe to return."""
    from condor_mcp import pool

    _two_schedds(monkeypatch, {}, {"a": [_job(JobStatus=4)], "b": ToolError("history of b down")})
    with pytest.raises(ToolError, match="history of b down"):
        pool.find_job(7, 0, ["JobStatus"])


async def test_job_details_shows_the_warning(monkeypatch):
    from condor_mcp import pool

    queued = _job(JobStatus=1, QDate=1000, Owner=pool.settings.user)
    _two_schedds(monkeypatch, {"a": [queued]}, {"b": ToolError("history of b down.")})
    job = await call("job_details", cluster_id=7)
    assert job["in_queue"] is True and job["schedd"] == "a"
    (warning,) = job["warnings"]
    assert warning.startswith("history of b down.")


def test_find_job_refuses_an_id_queued_on_two_schedds(monkeypatch):
    from condor_mcp import pool

    _two_schedds(monkeypatch, {"a": [_job(JobStatus=1, QDate=1000)], "b": [_job(JobStatus=2, QDate=900)]}, {})
    with pytest.raises(ToolError, match=r"Job 7\.0 exists on more than one schedd \(a, b\)"):
        pool.find_job(7, 0, ["JobStatus"])


def test_tail_of_one_long_line(tmp_path):
    """A last line longer than the cap comes back as its final MAX_OUTPUT_BYTES, marked cut."""
    from condor_mcp.server import MAX_OUTPUT_BYTES, _tail

    big = tmp_path / "big.out"
    big.write_text("x" * (2 * MAX_OUTPUT_BYTES) + "\n")
    text, size, truncated = _tail(big, 10)
    assert text == "x" * (MAX_OUTPUT_BYTES - 1) and size == 2 * MAX_OUTPUT_BYTES + 1 and truncated


@pytest.mark.parametrize("tail, first", [("", 904), ("end\n", 905)])
def test_tail_cut_at_a_line_start(tmp_path, tail, first):
    """Lines of 16 bytes put the 64 KB cut exactly at the start of line 904; a 4-byte
    last line moves it into line 904, which is then dropped as partial."""
    from condor_mcp.server import MAX_OUTPUT_BYTES, _tail

    f = tmp_path / "fixed.out"
    f.write_text("".join(f"{i:015d}\n" for i in range(5000)) + tail)
    text, size, truncated = _tail(f, 5000)
    lines = text.splitlines()
    assert lines[0] == f"{first:015d}" and truncated
    assert sum(len(line) + 1 for line in lines) <= MAX_OUTPUT_BYTES
