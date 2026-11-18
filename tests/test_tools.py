"""Every tool against the live pool, through FastMCP's in-memory client."""

from __future__ import annotations

import getpass

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from condor_mcp.server import mcp
from conftest import call

pytestmark = pytest.mark.anyio
ME = getpass.getuser()
NOBODY = "condor-mcp-nobody"


async def test_tool_annotations_split():
    """Clients use the annotations to decide what to confirm with the user."""
    read_only = {"job_details", "list_jobs", "hold_reasons", "job_history", "job_output", "pool_status", "user_priority", "draft_submit"}
    write = {"submit_job", "hold_job", "release_job", "remove_job"}
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert set(tools) == read_only | write
    for name, tool in tools.items():
        assert tool.annotations.read_only_hint is (name in read_only), name
        assert bool(tool.annotations.destructive_hint) is (name == "remove_job"), name


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
    assert job["in_queue"] is True and "hold" not in job and "ran_on" not in job


async def test_job_details_held(live_jobs):
    job = await call("job_details", cluster_id=live_jobs["held"], proc_id=0)
    assert job["status"] == "held"
    assert job["hold"]["code"] == 13
    assert "transfer_input_files" in job["hold"]["meaning"]
    assert "condor-mcp-test-input.csv" in job["hold"]["reason"]
    assert job["files"]["transfer_input"] == ["/nonexistent/condor-mcp-test-input.csv"]


async def test_job_details_finished(finished_job):
    job = await call("job_details", cluster_id=finished_job)
    assert job["status"] == "completed" and job["in_queue"] is False
    assert job["exit"]["code"] == 3 and job["exit"]["completed_at"]
    assert job["ran_on"] and job["running_on"] is None
    assert job["files"]["stdout"] == f"{finished_job}.0.out"


def test_job_details_no_ran_on_for_a_job_that_never_started():
    """A job whose input transfer failed keeps the slot it claimed in LastRemoteHost."""
    import classad2 as classad

    from condor_mcp.classads import summarize_job

    ad = classad.ClassAd({"ClusterId": 7, "ProcId": 0, "JobStatus": 3, "NumJobStarts": 0,
                          "LastRemoteHost": "slot1@host", "RemoveReason": "via condor_rm",
                          "LastHoldReason": "Transfer input files failure"})
    job = summarize_job(ad)
    assert "ran_on" not in job and job["last_hold_reason"] == "Transfer input files failure"


async def test_job_details_missing():
    with pytest.raises(ToolError, match="neither the queue nor the schedd history"):
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
    """Holds on any pool: only relations between the numbers, and the test's own running job."""
    out = await call("pool_status")
    assert out["machines"] >= 1
    assert out["slots"]["total"] == sum(out["slots"]["by_state"].values())
    assert out["slots"]["by_state"].get("Claimed", 0) >= 1
    for res in ("cpus", "memory_mb", "gpus"):
        assert 0 <= out[res]["available"] <= out[res]["total"]
    assert out["cpus"]["available"] < out["cpus"]["total"]  # the running job holds one
    by_model = out["gpus"]["by_model"].values()
    assert sum(m["total"] for m in by_model) == out["gpus"]["total"]
    assert sum(m["available"] for m in by_model) == out["gpus"]["available"]
    largest = out["largest_free_cpu_block"]
    assert 0 <= largest["free_cpus"] <= largest["machine_cpus"] <= out["cpus"]["total"]
    assert largest["free_cpus"] <= out["cpus"]["available"]
    assert "slot_list" not in out


async def test_pool_status_schedd_health(live_jobs):
    out = await call("pool_status")
    [schedd] = out["schedds"]  # the local one
    assert 0 <= schedd["duty_cycle"] and schedd["load"] in {"ok", "busy", "strained", "overloaded"}
    assert schedd["jobs"]["running"] >= 1 and schedd["jobs"]["held"] >= 1
    assert schedd["jobs"]["running"] <= schedd["max_running"]


@pytest.mark.parametrize("duty, load", [(0.0, "ok"), (0.49, "ok"), (0.5, "busy"), (0.8, "strained"), (0.95, "overloaded")])
def test_schedd_load_names(duty, load):
    import classad2 as classad

    from condor_mcp import classads

    ad = classad.ClassAd({"RecentDaemonCoreDutyCycle": duty, "TotalRunningJobs": 5, "MaxJobsRunning": 10})
    row = classads.schedd_health("s", ad)
    assert row["load"] == load and "note" not in row


def test_schedd_at_its_running_cap():
    import classad2 as classad

    from condor_mcp import classads

    row = classads.schedd_health("s", classad.ClassAd({"TotalRunningJobs": 10, "MaxJobsRunning": 10}))
    assert "MaxJobsRunning cap" in row["note"] and "load" not in row
    assert "may be down" in classads.schedd_health("gone", None)["note"]


async def test_pool_status_without_schedd_ads(monkeypatch):
    """A failed schedd lookup leaves the slot numbers and says why the schedds are missing."""
    from condor_mcp import pool

    def fail():
        raise ToolError("Could not read the schedd ads")

    monkeypatch.setattr(pool, "schedd_ads", fail)
    out = await call("pool_status")
    assert out["slots"]["total"] >= 1 and "schedds" not in out
    assert out["warnings"] == ["Could not read the schedd ads"]


async def test_pool_status_detail(live_jobs):
    out = await call("pool_status", detail=True)
    slots = out["slot_list"]
    assert len(slots) == out["slots"]["total"]
    claimed = [s for s in slots if s["state"] == "Claimed"]
    assert any((s["claimed_by"] or "").startswith(ME) for s in claimed)
    assert all("gpu_model" in s for s in slots)


async def test_pool_status_gpu_models(monkeypatch):
    """GPU models come from GPUs_DeviceName, the name current HTCondor publishes, and
    CUDADeviceName on older pools. The fake collector returns only projected attributes,
    as the real one does."""
    import classad2 as classad

    from condor_mcp import pool

    a100 = {"Name": "slot1@gpu1", "Machine": "gpu1", "State": "Unclaimed", "PartitionableSlot": True,
            "Cpus": 8, "Memory": 4096, "GPUs": 1, "TotalSlotCpus": 32, "TotalSlotMemory": 65536,
            "TotalSlotGPUs": 4, "GPUs_DeviceName": "NVIDIA A100"}
    old = {"Name": "slot1@gpu2", "Machine": "gpu2", "State": "Unclaimed", "PartitionableSlot": True,
           "Cpus": 4, "Memory": 8192, "GPUs": 2, "TotalSlotCpus": 4, "TotalSlotMemory": 8192,
           "TotalSlotGPUs": 2, "CUDADeviceName": "Tesla K80"}

    def fake_query_slots(projection, constraint=None):
        return [classad.ClassAd({k: v for k, v in ad.items() if k in projection}) for ad in (a100, old)]

    monkeypatch.setattr(pool, "query_slots", fake_query_slots)
    out = await call("pool_status", detail=True)
    assert out["gpus"] == {"total": 6, "available": 3, "by_model": {
        "NVIDIA A100": {"total": 4, "available": 1}, "Tesla K80": {"total": 2, "available": 2}}}
    assert [s["gpu_model"] for s in out["slot_list"]] == ["NVIDIA A100", "Tesla K80"]


async def test_job_details_requested_gpus():
    """RequestGPUs is fetched; the job is submitted on hold so it never needs a GPU slot."""
    import htcondor2 as htcondor

    schedd = htcondor.Schedd()
    cluster = schedd.submit(htcondor.Submit({
        "executable": "/bin/sleep", "arguments": "60", "request_gpus": "2", "hold": "true",
        "request_memory": "64MB", "request_disk": "10MB", "+CondorMcpTest": "true",
    })).cluster()
    try:
        job = await call("job_details", cluster_id=cluster, proc_id=0)
        assert job["requested"]["gpus"] == 2
    finally:
        schedd.act(htcondor.JobAction.Remove, f"ClusterId == {cluster}")


async def test_entry_point_over_stdio():
    """`uv run condor-mcp` is what .mcp.json launches."""
    from fastmcp.client.transports import StdioTransport

    async with Client(StdioTransport("uv", ["run", "condor-mcp"])) as client:
        names = {t.name for t in await client.list_tools()}
    assert "pool_status" in names
