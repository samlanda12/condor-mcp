"""Live-pool fixtures. Run on a submit host as a user allowed to submit.

Submits one job that runs (sleep) and one that goes on hold (missing input
file, hold code 13), waits for both states, and removes them afterwards.
Only jobs tagged CondorMcpTest are touched."""

from __future__ import annotations

import time

import htcondor2 as htcondor
import pytest

TAG = "CondorMcpTest"


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
