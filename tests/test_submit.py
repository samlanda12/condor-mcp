"""draft_submit against the live pool. Nothing here queues a job."""

from __future__ import annotations


import htcondor2 as htcondor
import pytest
from fastmcp.exceptions import ToolError

from conftest import WORKDIR, call

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
