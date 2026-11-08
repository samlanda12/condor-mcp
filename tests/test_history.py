"""job_history against the live pool: a short job that exits 3, plus the history
as a whole."""

from __future__ import annotations

import getpass

import pytest

from conftest import call

ME = getpass.getuser()
NOBODY = "condor-mcp-nobody"

pytestmark = pytest.mark.anyio


async def test_job_history_completed(finished_job):
    out = await call("job_history", cluster_id=finished_job)
    assert out["owner"] == ME and out["truncated"] is False and "note" not in out
    (row,) = out["jobs"]
    assert row["job_id"] == f"{finished_job}.0" and row["status"] == "completed"
    assert row["exit"] == {"code": 3, "by_signal": False, "signal": None}
    assert row["command"].endswith("condor-mcp-print.sh")
    assert row["ran_on"] and row["submitted_at"] and row["finished_at"]
    assert row["starts"] == 1
    assert row["memory_mb"]["requested"] == 64
    assert "remove_reason" not in row


async def test_job_history_newest_first_and_limit(finished_job):
    out = await call("job_history", limit=2)
    assert len(out["jobs"]) == 2 and out["truncated"]
    first, second = out["jobs"]
    assert first["finished_at"] >= second["finished_at"]


async def test_job_history_missing_cluster():
    out = await call("job_history", cluster_id=999999)
    assert out["jobs"] == [] and out["truncated"] is False
    assert out["note"].startswith(f"Cluster 999999 is not in the history for {ME}.")


async def test_job_history_other_owner():
    out = await call("job_history", owner=NOBODY)
    assert out == {
        "owner": NOBODY, "jobs": [], "truncated": False,
        "note": f"No finished jobs in the history for {NOBODY}.",
    }
