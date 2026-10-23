"""The submit tools against the live pool. Only the submitted_job fixture queues
anything, and it removes what it queued."""

from __future__ import annotations

import getpass
import os
import subprocess
import sys

import htcondor2 as htcondor
import pytest
from fastmcp.exceptions import ToolError

from conftest import TAG, WORKDIR, call

ME = getpass.getuser()

pytestmark = pytest.mark.anyio


async def test_draft_sleep_job():
    out = await call(
        "draft_submit", executable="/bin/sleep", arguments="30", memory_mb=64, disk_mb=10,
        extra={"+CondorMcpTest": "true"},
    )
    text = out["submit_text"]
    for line in (
        "executable              = /bin/sleep",
        "arguments               = 30",
        "request_cpus            = 1",
        "request_memory          = 64MB",
        "request_disk            = 10MB",
        f"initialdir              = {WORKDIR}",
        "output                  = $(ClusterId).$(ProcId).out",
        "log                     = $(ClusterId).log",
        "+CondorMcpTest          = true",
        "\nqueue 1\n",
    ):
        assert line in text, line
    assert "transfer_input_files" not in text and "request_gpus" not in text
    assert htcondor.Submit(text).getQArgs() == "1"
    assert out["work_dir"] == str(WORKDIR)
    assert out["warnings"] == []
    assert out["submittable"] is True and out["jobs"] == 1
    assert "TARGET.Memory >= RequestMemory" in out["requirements"]
    assert out["slots"]["fit_requirements"] >= 1
    assert out["slots"]["would_accept"] >= 1
    assert out["slots"]["largest_slot"]["cpus"] >= 1


async def test_draft_queue_over_items_checks_each_input(tmp_path):
    present = tmp_path / "a.csv"
    present.write_text("1\n")
    out = await call(
        "draft_submit", executable="/bin/cat", arguments="$(item)", input_files=["$(item)"],
        queue=[str(present), "missing.csv"],
    )
    text = out["submit_text"]
    assert "transfer_input_files    = $(item)" in text
    assert f"queue item from (\n    {present}\n    missing.csv\n)\n" in text
    assert out["jobs"] == 2 and out["submittable"] is True
    assert len(out["warnings"]) == 1
    assert "missing.csv" in out["warnings"][0] and str(present) not in out["warnings"][0]
    assert "hold" in out["warnings"][0]


def test_workdir_is_created_at_startup(tmp_path):
    """A fresh process, since conftest already created the test workdir."""
    new = tmp_path / "jobs" / "nested"
    env = os.environ | {"CONDOR_MCP_WORKDIR": str(new)}
    subprocess.run([sys.executable, "-c", "import condor_mcp.pool"], env=env, check=True, timeout=60)
    assert new.is_dir()


async def test_draft_missing_executable():
    out = await call("draft_submit", executable="run.sh")
    assert f"executable              = {WORKDIR / 'run.sh'}" in out["submit_text"]
    assert out["submittable"] is False and out["jobs"] == 0 and out["slots"] is None
    joined = "\n".join(out["warnings"])
    assert "does not exist" in joined
    assert "Can't access executable" in joined
    assert "condor-mcp-draft" not in joined  # the temp submit file is not the user's business


async def test_draft_typo_in_extra():
    out = await call(
        "draft_submit", executable="/bin/sleep", extra={"transfer_input_file": "/etc/hostname"},
    )
    assert out["submittable"] is True
    assert len(out["warnings"]) == 1
    assert "typo" in out["warnings"][0] and "transfer_input_file" in out["warnings"][0]


async def test_draft_impossible_request():
    out = await call("draft_submit", executable="/bin/sleep", gpus=1, memory_mb=64)
    assert "request_gpus            = 1" in out["submit_text"]
    assert out["submittable"] is True
    assert "TARGET.GPUs >= RequestGPUs" in out["requirements"]
    assert out["slots"]["fit_requirements"] == 0  # the dev pool has no GPUs
    assert any("No slot" in w and "1 GPUs" in w for w in out["warnings"])


async def test_draft_bad_expression():
    out = await call("draft_submit", executable="/bin/sleep", extra={"request_memroy": "64MB"})
    assert out["submittable"] is False and out["jobs"] == 0
    assert out["warnings"] == ["ERROR: Parse error in expression: Requestmemroy = 64MB"]


@pytest.mark.parametrize(
    "args",
    [
        # submit's include and "queue from command |" run commands at parse time
        {"extra": {"include : touch /tmp/condor-mcp-pwned #": "|"}},
        {"extra": {"requirements\ninclude : touch /tmp/condor-mcp-pwned |": "true"}},
        {"extra": {"requirements": "true\ninclude : touch /tmp/condor-mcp-pwned |"}},
        {"extra": {"request memory": "64MB"}},
        {"arguments": "1\ninclude : touch /tmp/condor-mcp-pwned |"},
        {"input_files": ["a\ninclude : touch /tmp/condor-mcp-pwned |"]},
        {"queue": ["a", ")\nqueue item from touch /tmp/condor-mcp-pwned |\n#"]},
    ],
)
async def test_draft_rejects_second_statement(args):
    with pytest.raises(ToolError, match="one line|not a submit command"):
        await call("draft_submit", executable="/bin/sleep", **args)


async def test_submit_dry_run_of_draft(submitted_job):
    out = await call("submit_job", submit_text=submitted_job["submit_text"], dry_run=True)
    assert out["dry_run"] is True and out["jobs"] == 1 and out["warnings"] == []
    assert "cluster_id" not in out
    assert out["submit_dir"] == str(WORKDIR)
    (row,) = out["job_ads"]
    assert row["proc"] == 0
    assert row["command"] == "/bin/sleep 600"
    assert row["submit_dir"] == str(WORKDIR)
    assert row["requested"]["cpus"] == 1 and row["requested"]["memory_mb"] == 64
    assert row["requested"]["gpus"] == 0
    # HTCondor keeps these relative to submit_dir
    assert row["files"]["stdout"].endswith(".0.out") and row["files"]["log"].endswith(".log")
    assert "TARGET.Memory >= RequestMemory" in row["requirements"]


async def test_submit_dry_run_of_file_with_relative_paths(tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/sh\necho $1\n")
    script.chmod(0o755)
    (tmp_path / "in.txt").write_text("x\n")
    sub = tmp_path / "job.sub"
    sub.write_text(
        "executable = run.sh\narguments = $(item)\ntransfer_input_files = in.txt\n"
        f"request_memory = 64MB\n+{TAG} = true\nqueue item from (\n a\n b\n)\n"
    )
    out = await call("submit_job", submit_file=str(sub), dry_run=True)
    assert out["jobs"] == 2 and out["submit_dir"] == str(tmp_path)
    first, second = out["job_ads"]
    assert first["command"] == f"{script} a" and second["command"] == f"{script} b"
    assert first["submit_dir"] == str(tmp_path)
    assert first["files"]["transfer_input"] == ["in.txt"]
    assert second["requested"] == first["requested"]


async def test_submit_dry_run_of_file_with_relative_initialdir(tmp_path):
    run_dir = tmp_path / "sub"
    run_dir.mkdir()
    script = tmp_path / "run.sh"  # condor_submit takes executable against its cwd, not initialdir
    script.write_text("#!/bin/sh\necho hi\n")
    script.chmod(0o755)
    sub = tmp_path / "job.sub"
    sub.write_text(f"initialdir = sub\nexecutable = run.sh\nrequest_memory = 64MB\n+{TAG} = true\nqueue 1\n")
    out = await call("submit_job", submit_file=str(sub), dry_run=True)
    assert out["submit_dir"] == str(run_dir) and out["warnings"] == []
    (row,) = out["job_ads"]
    assert row["submit_dir"] == str(run_dir)
    assert row["command"] == str(script)


async def test_submit_job_queues_it(submitted_job):
    result = submitted_job["result"]
    cluster = submitted_job["cluster"]
    assert result["jobs"] == 1 and result["warnings"] == []
    assert result["submit_dir"] == str(WORKDIR)
    (row,) = result["job_list"]
    assert row["job_id"] == f"{cluster}.0" and row["owner"] == ME
    assert row["status"] in ("idle", "running")
    job = await call("job_details", cluster_id=cluster)
    assert job["command"] == "/bin/sleep 600"
    assert job["submit_dir"] == str(WORKDIR)
    assert job["files"]["stdout"] == f"{cluster}.0.out"  # HTCondor keeps Out relative to submit_dir
    assert job["files"]["log"] == str(WORKDIR / f"{cluster}.log")  # but makes UserLog absolute
    assert job["requested"]["memory_mb"] == 64
    assert "hold" not in job


async def test_submit_refuses_missing_executable():
    with pytest.raises(ToolError, match="Refusing to submit.*Can't access executable"):
        await call("submit_job", submit_text=f"executable = /nonexistent/x\n+{TAG} = true\nqueue 1\n")


async def test_submit_refuses_missing_initialdir():
    with pytest.raises(ToolError, match="initialdir /nonexistent/d does not exist"):
        await call("submit_job", submit_text="executable = /bin/sleep\ninitialdir = /nonexistent/d\nqueue 1\n")


@pytest.mark.parametrize(
    "text",
    [
        "include : /etc/hostname\nexecutable = /bin/sleep\nqueue 1\n",
        "executable = /bin/sleep\nINCLUDE command : touch /tmp/condor-mcp-should-not-exist |\nqueue 1\n",
        "executable = /bin/sleep\nqueue item from touch /tmp/condor-mcp-should-not-exist |\n",
    ],
)
async def test_submit_refuses_command_statements(text):
    with pytest.raises(ToolError, match="lines are not accepted"):
        await call("submit_job", submit_text=text)


async def test_submit_allows_macro_named_like_include():
    text = f"include_dir = /bin\nexecutable = $(include_dir)/sleep\n+{TAG} = true\nqueue 1\n"
    out = await call("submit_job", submit_text=text, dry_run=True)
    assert out["job_ads"][0]["command"] == "/bin/sleep"


async def test_submit_argument_errors(tmp_path):
    with pytest.raises(ToolError, match="exactly one"):
        await call("submit_job")
    with pytest.raises(ToolError, match="exactly one"):
        await call("submit_job", submit_text="queue 1\n", submit_file="x.sub")
    with pytest.raises(ToolError, match="Could not read submit file"):
        await call("submit_job", submit_file=str(tmp_path / "missing.sub"))
