"""Every tool against the live pool, through FastMCP's in-memory client."""

from __future__ import annotations

import getpass

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from condor_mcp.server import mcp

pytestmark = pytest.mark.anyio
ME = getpass.getuser()
NOBODY = "condor-mcp-nobody"


async def call(name: str, **args):
    async with Client(mcp) as client:
        return (await client.call_tool(name, args)).data


async def test_all_tools_are_read_only():
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert set(tools) == {"job_details", "list_jobs", "hold_reasons", "pool_status"}
    for tool in tools.values():
        assert tool.annotations.read_only_hint is True, tool.name


async def test_job_details_running(live_jobs):
    job = await call("job_details", cluster_id=live_jobs["running"])
    assert job["job_id"] == f"{live_jobs['running']}.0"
    assert job["status"] == "running"
    assert job["owner"] == ME
    assert job["running_on"]
    assert job["started_at"] and job["status_since"]
    assert job["command"] == "/bin/sleep 900"
    assert job["requested"]["memory_mb"] == 64
    assert isinstance(job["requested"]["disk_kb"], int)
    assert job["files"]["transfer_input"] == []
    assert "hold" not in job


async def test_job_details_held(live_jobs):
    job = await call("job_details", cluster_id=live_jobs["held"], proc_id=0)
    assert job["status"] == "held"
    assert job["hold"]["code"] == 13
    assert "transfer_input_files" in job["hold"]["meaning"]
    assert "condor-mcp-test-input.csv" in job["hold"]["reason"]
    assert job["files"]["transfer_input"] == ["/nonexistent/condor-mcp-test-input.csv"]


async def test_job_details_missing():
    with pytest.raises(ToolError, match="not in the queue"):
        await call("job_details", cluster_id=999999, proc_id=0)


async def test_list_jobs_mine(live_jobs):
    out = await call("list_jobs")
    assert out["owner"] == ME
    ids = {row["job_id"]: row for row in out["jobs"]}
    running = ids[f"{live_jobs['running']}.0"]
    held = ids[f"{live_jobs['held']}.0"]
    assert running["status"] == "running" and running["running_on"]
    assert held["status"] == "held" and held["hold_code"] == 13
    assert out["counts_by_status"]["running"] >= 1
    assert out["counts_by_status"]["held"] >= 1
    assert out["truncated"] is False
    assert "note" not in out


async def test_list_jobs_status_filter_and_limit(live_jobs):
    out = await call("list_jobs", owner="all", status="running")
    assert out["owner"] == "all"
    assert out["jobs"] and all(row["status"] == "running" for row in out["jobs"])
    out = await call("list_jobs", owner="all", limit=1)
    assert len(out["jobs"]) == 1
    assert out["truncated"] is True


async def test_list_jobs_empty():
    out = await call("list_jobs", owner=NOBODY)
    assert out["jobs"] == []
    assert out["counts_by_status"] == {}
    assert out["truncated"] is False
    assert NOBODY in out["note"]


async def test_hold_reasons(live_jobs):
    out = await call("hold_reasons")
    assert out["owner"] == ME
    assert out["count"] >= 1
    row = next(r for r in out["held_jobs"] if r["job_id"] == f"{live_jobs['held']}.0")
    assert row["code"] == 13 and row["subcode"] is not None
    assert row["held_since"]
    assert "missing or unreadable" in row["meaning"]
    assert row["transfer_input"] == ["/nonexistent/condor-mcp-test-input.csv"]
    assert any(entry["code"] == 13 and entry["count"] >= 1 for entry in out["by_code"])
    assert "note" not in out


async def test_hold_reasons_empty():
    out = await call("hold_reasons", owner=NOBODY)
    assert out["count"] == 0 and out["held_jobs"] == [] and out["by_code"] == []
    assert NOBODY in out["note"]


async def test_pool_status(live_jobs):
    out = await call("pool_status")
    assert out["machines"] >= 1
    assert out["slots"]["total"] >= 2  # partitionable slot + the running job's dynamic slot
    assert out["slots"]["by_state"].get("Claimed", 0) >= 1
    assert out["cpus"]["total"] > 0
    assert out["cpus"]["available"] < out["cpus"]["total"]
    assert out["memory_mb"]["available"] < out["memory_mb"]["total"]
    assert out["gpus"]["total"] == 0 and out["gpus"]["by_model"] == {}
    assert out["largest_free_cpu_block"]["machine_cpus"] == out["cpus"]["total"]
    assert "slot_list" not in out


async def test_pool_status_detail(live_jobs):
    out = await call("pool_status", detail=True)
    slots = out["slot_list"]
    assert len(slots) == out["slots"]["total"]
    claimed = [s for s in slots if s["state"] == "Claimed"]
    assert claimed and claimed[0]["claimed_by"].startswith(ME)
    assert all(s["gpu_model"] is None for s in slots)


async def test_entry_point_over_stdio():
    """`uv run condor-mcp` is what .mcp.json launches."""
    from fastmcp.client.transports import StdioTransport

    async with Client(StdioTransport("uv", ["run", "condor-mcp"])) as client:
        names = {t.name for t in await client.list_tools()}
    assert "pool_status" in names
