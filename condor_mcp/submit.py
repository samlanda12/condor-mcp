"""Build and check submit files without queuing anything.

Three checks: the htcondor2.Submit parser, condor_submit -dry-run (generates the
job ads without contacting the schedd), and a match of the first ad against every
slot in the pool at full size.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import classad2 as classad
import htcondor2 as htcondor
from fastmcp.exceptions import ToolError

from condor_mcp import pool
from condor_mcp.classads import get

ITEM = "$(item)"
RESOURCES = ("Cpus", "Memory", "Disk", "GPUs")
# A submit command or a +Attribute. See compose() for why keys are restricted.
COMMAND = re.compile(r"^\+?[A-Za-z_][A-Za-z0-9_.]*$")


def _multiline(value: str) -> bool:
    return "\n" in value or "\r" in value


def resolve(path: str, workdir: Path) -> str:
    """Absolute path under workdir; paths with macros are left alone."""
    path = os.path.expanduser(path)
    if "$(" in path or os.path.isabs(path):
        return path
    return str(workdir / path)


def compose(
    *,
    executable: str,
    arguments: str,
    input_files: list[str],
    output_files: list[str],
    cpus: int,
    memory_mb: int,
    disk_mb: int | None,
    gpus: int,
    queue: int | list[str],
    extra: dict[str, str],
    workdir: Path,
) -> str:
    """Extra commands go last so they override: HTCondor keeps the last setting.

    Every value is held to one line and every key to a command name, so no field
    can sneak in a second statement."""
    lines: list[tuple[str, str]] = [("executable", executable)]
    if arguments:
        lines.append(("arguments", arguments))
    if input_files:
        lines.append(("transfer_input_files", ", ".join(input_files)))
    if output_files:
        lines.append(("transfer_output_files", ", ".join(output_files)))
    lines += [
        ("should_transfer_files", "YES"),
        ("when_to_transfer_output", "ON_EXIT"),
        ("request_cpus", str(cpus)),
        ("request_memory", f"{memory_mb}MB"),
    ]
    if disk_mb is not None:
        lines.append(("request_disk", f"{disk_mb}MB"))
    if gpus:
        lines.append(("request_gpus", str(gpus)))
    lines += [
        ("initialdir", str(workdir)),
        ("output", "$(ClusterId).$(ProcId).out"),
        ("error", "$(ClusterId).$(ProcId).err"),
        ("log", "$(ClusterId).log"),
    ]
    lines += list(extra.items())
    for key, value in lines:
        if not COMMAND.match(key):
            raise ToolError(f"extra command {key!r} is not a submit command name or +Attribute.")
        if _multiline(value):
            raise ToolError(f"{key} must be one line.")
    if not isinstance(queue, int) and any(_multiline(item) for item in queue):
        raise ToolError("Each queue item must be one line.")
    width = max(len(key) for key, _ in lines)
    text = "".join(f"{key.ljust(width)} = {value}\n" for key, value in lines)
    if isinstance(queue, int):
        return f"{text}\nqueue {queue}\n"
    items = "".join(f"    {item}\n" for item in queue)
    return f"{text}\nqueue item from (\n{items})\n"


def check_files(
    executable: str, input_files: list[str], queue: int | list[str], workdir: Path
) -> list[str]:
    """Checks condor_submit skips: workdir, the executable bit, and input files
    (a missing one puts the job on hold with code 13 at transfer time)."""
    warnings: list[str] = []
    if not workdir.is_dir():
        warnings.append(
            f"The work directory {workdir} does not exist. Create it before submitting; "
            "the job's log, stdout and stderr go there."
        )
    if "$(" not in executable:
        if not os.path.isfile(executable):
            warnings.append(f"Executable {executable} does not exist.")
        elif not os.access(executable, os.X_OK):
            warnings.append(f"Executable {executable} is not executable; chmod +x it.")
    items = queue if isinstance(queue, list) else []
    missing = []
    for entry in input_files:
        expanded = [entry.replace(ITEM, item) for item in items] if ITEM in entry else [entry]
        for path in expanded:
            if "$(" not in path and not os.path.exists(resolve(path, workdir)):
                missing.append(path)
    if missing:
        warnings.append(
            "Input files not found: " + ", ".join(missing) + ". The job would be submitted "
            "but go on hold (code 13) when HTCondor tries to transfer them."
        )
    return warnings


def parse(text: str) -> str | None:
    """Parse error message, or None."""
    try:
        htcondor.Submit(text)
    except (ValueError, htcondor.HTCondorException) as exc:
        return f"HTCondor could not parse the submit text: {exc}"
    return None


def dry_run(text: str) -> tuple[list[classad.ClassAd], list[str]]:
    """Job ads from condor_submit -dry-run plus its errors and warnings.
    Only the first ad is complete; the rest hold the attributes that differ."""
    condor_submit = shutil.which("condor_submit")
    if not condor_submit:
        return [], ["condor_submit is not on PATH, so the draft was parsed but not dry-run."]
    with tempfile.TemporaryDirectory(prefix="condor-mcp-draft-") as tmp:
        sub = Path(tmp, "draft.sub")
        sub.write_text(text)
        ads_file = Path(tmp, "dry-run.ads")
        try:
            proc = subprocess.run(
                [condor_submit, "-dry-run", str(ads_file), str(sub)],
                cwd=tmp, capture_output=True, text=True, timeout=60,
            )
        except subprocess.TimeoutExpired:
            return [], ["condor_submit -dry-run did not finish within 60 seconds."]
        # stdout's progress line has no trailing newline, so keep the streams apart
        messages = _messages(proc.stdout + "\n" + proc.stderr, str(sub))
        if proc.returncode != 0:
            return [], messages or [f"condor_submit -dry-run failed with exit code {proc.returncode}."]
        ads = _parse_ads(ads_file.read_text()) if ads_file.exists() else []
    return ads, messages


def _messages(output: str, sub_path: str) -> list[str]:
    """ERROR/WARNING lines with their continuations folded in; progress lines dropped."""
    messages: list[str] = []
    for line in output.replace(sub_path, "the draft").splitlines():
        if not line.strip() or line.startswith("Dry-Run job(s)") or "dry-run to cluster" in line:
            continue
        if line.startswith(("ERROR", "WARNING")) or not messages:
            messages.append(line.strip())
        else:
            messages[-1] += " " + line.strip()
    return messages


def _parse_ads(text: str) -> list[classad.ClassAd]:
    body = "\n".join(line for line in text.splitlines() if "=" in line or not line.strip())
    return list(classad.parseAds(body, classad.ParserType.Old))


def slot_check(job: classad.ClassAd) -> dict[str, Any]:
    """Match the job against every non-dynamic slot sized to its total resources.
    fit_requirements: slots the job's Requirements accept. would_accept: those
    whose START also accepts the job."""
    slots = [ad for ad in pool.query_slots([]) if not get(ad, "DynamicSlot", False)]
    fit = accept = 0
    largest = dict.fromkeys(RESOURCES, 0)
    for slot in slots:
        for res in RESOURCES:
            total = int(get(slot, f"TotalSlot{res}", get(slot, res, 0)) or 0)
            slot[res] = total
            largest[res] = max(largest[res], total)
        if slot.matches(job):
            fit += 1
            if job.matches(slot):
                accept += 1
    return {
        "slots": len(slots),
        "fit_requirements": fit,
        "would_accept": accept,
        "largest_slot": {
            "cpus": largest["Cpus"],
            "memory_mb": largest["Memory"],
            "disk_mb": largest["Disk"] // 1024,
            "gpus": largest["GPUs"],
        },
    }
