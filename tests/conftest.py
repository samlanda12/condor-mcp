"""Live-pool fixtures. Run on a submit host as a user allowed to submit.

Submits one job that runs (sleep) and one that goes on hold (missing input
file, hold code 13), waits for both states, and removes them afterwards.
A third job is drafted and queued through the submit tools. Only jobs tagged
CondorMcpTest are touched. Drafted jobs use a work directory under the system
temp dir."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Any

import htcondor2 as htcondor
import pytest

TAG = "CondorMcpTest"

# Drafted jobs run from here. Set before the server reads its settings, overriding
# any value in the shell so the tests and the server agree on the directory.
WORKDIR = Path(tempfile.gettempdir()) / "condor-mcp-tests"
WORKDIR.mkdir(exist_ok=True)
os.environ["CONDOR_MCP_WORKDIR"] = str(WORKDIR)


async def call(name: str, **args: Any) -> Any:
    """Call one tool through FastMCP's in-memory client and return its data."""
    from fastmcp import Client

    from condor_mcp.server import mcp

    async with Client(mcp) as client:
        return (await client.call_tool(name, args)).data


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


def _wait_for_status(schedd: htcondor.Schedd, cluster: int, status: int, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ads = schedd.query(f"ClusterId == {cluster}", projection=["JobStatus"])
        if ads and ads[0]["JobStatus"] == status:
            return
        time.sleep(2)
    raise RuntimeError(f"cluster {cluster} did not reach JobStatus {status} within {timeout}s")


@pytest.fixture(scope="session")
def live_jobs():
    schedd = htcondor.Schedd()
    common = {f"+{TAG}": "true", "should_transfer_files": "YES", "transfer_output_files": '""',
              "request_memory": "64MB", "request_disk": "10MB"}
    running = schedd.submit(htcondor.Submit({
        "executable": "/bin/sleep", "arguments": "900", **common,
    })).cluster()
    held = schedd.submit(htcondor.Submit({
        "executable": "/bin/cat",
        "transfer_input_files": "/nonexistent/condor-mcp-test-input.csv",
        **common,
    })).cluster()
    try:
        _wait_for_status(schedd, running, 2)
        _wait_for_status(schedd, held, 5)
        yield {"running": running, "held": held}
    finally:
        schedd.act(htcondor.JobAction.Remove, f"{TAG} == true")


@pytest.fixture(scope="session")
async def submitted_job(anyio_backend):
    """A sleep job drafted with draft_submit and queued with submit_job."""
    draft = await call(
        "draft_submit", executable="/bin/sleep", arguments="600", memory_mb=64, disk_mb=10,
        extra={f"+{TAG}": "true"},
    )
    assert draft["submittable"] and draft["warnings"] == [], draft
    out = await call("submit_job", submit_text=draft["submit_text"])
    try:
        yield {"cluster": out["cluster_id"], "submit_text": draft["submit_text"], "result": out}
    finally:
        htcondor.Schedd().act(htcondor.JobAction.Remove, f"ClusterId == {out['cluster_id']}")
