"""hold_job, release_job, and remove_job against the live pool, on a cluster
queued for the purpose and removed afterwards."""

from __future__ import annotations

import getpass
import time

import anyio
import pytest
from fastmcp.exceptions import ToolError

from conftest import call

ME = getpass.getuser()

pytestmark = pytest.mark.anyio


async def _wait_until_gone(cluster: int, timeout: float = 60) -> None:
    """A removed job can sit in the queue as 'removed' while its shadow exits."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            await call("job_details", cluster_id=cluster)
        except ToolError as exc:
            assert "not in the queue" in str(exc)
            return
        await anyio.sleep(2)
    raise AssertionError(f"cluster {cluster} still in the queue after {timeout}s")


async def test_hold_release_remove(fresh_job):
    held = await call("hold_job", cluster_id=fresh_job, reason="pausing for a test")
    assert held["action"] == "hold" and held["reason"] == "pausing for a test"
    assert held["changed"] == 1 and "note" not in held
    (row,) = held["jobs"]
    assert row["job_id"] == f"{fresh_job}.0"
    assert row["status_before"] in ("idle", "running") and row["status_after"] == "held"
    assert row["hold_reason"] == f"pausing for a test (by user {ME})"
    job = await call("job_details", cluster_id=fresh_job)
    assert job["status"] == "held"
    assert job["hold"]["code"] == 1 and job["hold"]["reason"] == f"pausing for a test (by user {ME})"

    again = await call("hold_job", cluster_id=fresh_job, reason="a second time")
    assert again["changed"] == 0 and again["note"] == f"Nothing to hold: {fresh_job}.0 is held."
    assert again["jobs"][0]["status_before"] == "held"

    released = await call("release_job", cluster_id=fresh_job, proc_id=0, reason="test is over")
    assert released["action"] == "release" and released["changed"] == 1
    (row,) = released["jobs"]
    assert row["status_before"] == "held" and row["status_after"] == "idle"
    assert "hold_reason" not in row
    job = await call("job_details", cluster_id=fresh_job)
    assert job["status"] in ("idle", "running") and "hold" not in job

    removed = await call("remove_job", cluster_id=fresh_job, reason="done with it")
    assert removed["action"] == "remove" and removed["changed"] == 1
    (row,) = removed["jobs"]
    assert row["status_before"] in ("idle", "running") and row["status_after"] == "removed"
    await _wait_until_gone(fresh_job)

    again = await call("remove_job", cluster_id=fresh_job, reason="a second time")
    assert again["changed"] == 0 and again["jobs"] == []
    assert again["note"].startswith(f"Nothing to remove: cluster {fresh_job} is not in the queue")


async def test_remove_of_a_missing_job_is_not_an_error():
    out = await call("remove_job", cluster_id=999999, proc_id=3, reason="no such job")
    assert out["changed"] == 0 and out["jobs"] == []
    assert "job 999999.3 is not in the queue" in out["note"] and "ID may be wrong" in out["note"]


async def test_release_of_a_job_not_held(fresh_job):
    out = await call("release_job", cluster_id=fresh_job, reason="nothing to do")
    assert out["changed"] == 0 and out["note"].startswith("Nothing to release")
    (row,) = out["jobs"]
    assert row["status_before"] == row["status_after"] in ("idle", "running")


async def test_act_on_missing_job():
    with pytest.raises(ToolError, match="Cluster 999999 is not in the queue"):
        await call("hold_job", cluster_id=999999, reason="no such job")


async def test_act_on_missing_proc(fresh_job):
    with pytest.raises(ToolError, match=f"Job {fresh_job}.7 is not in the queue"):
        await call("hold_job", cluster_id=fresh_job, proc_id=7, reason="no such proc")


async def test_act_refuses_another_users_job(fresh_job, monkeypatch):
    from condor_mcp import pool, server

    as_other = pool.settings.model_copy(update={"user": "someone-else"})
    monkeypatch.setattr(pool, "settings", as_other)
    monkeypatch.setattr(server, "settings", as_other)
    with pytest.raises(ToolError, match=f"Refusing to hold cluster {fresh_job}: it belongs to {ME}, not someone-else"):
        await call("hold_job", cluster_id=fresh_job, reason="not mine")
    with pytest.raises(ToolError, match="Refusing to release"):
        await call("release_job", cluster_id=fresh_job, reason="not mine")
    with pytest.raises(ToolError, match="Refusing to remove"):
        await call("remove_job", cluster_id=fresh_job, reason="not mine")
    job = await call("job_details", cluster_id=fresh_job)
    assert job["status"] in ("idle", "running") and "hold" not in job


@pytest.mark.parametrize("args", [{"reason": ""}, {}, {"proc_id": -1, "reason": "x"}])
async def test_act_argument_errors(args):
    with pytest.raises(ToolError):
        await call("hold_job", cluster_id=1, **args)
