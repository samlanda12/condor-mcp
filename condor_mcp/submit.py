"""Build, check, and queue submit files.

Three checks: the htcondor2.Submit parser, condor_submit -dry-run (generates the
job ads without contacting the schedd), and a match of the first ad against every
slot in the pool at full size. submit_job runs the dry run before every real
submit, since the Python bindings queue a job whose executable does not exist.
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

from condor_mcp import classads, pool
from condor_mcp.classads import get

ITEM = "$(item)"
RESOURCES = ("Cpus", "Memory", "Disk", "GPUs")
# A submit command or a +Attribute. See compose() for why keys are restricted.
COMMAND = re.compile(r"\+?[A-Za-z_][A-Za-z0-9_.]*")
# condor_submit joins a line ending in a backslash (trailing blanks allowed) with the next, keyword included.
CONTINUATION = re.compile(r"\\\s*\n\s*")


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
    can sneak in a second statement, and no queue item may close the item list,
    since condor_submit reads whatever follows as further statements."""
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
        if not COMMAND.fullmatch(key):
            raise ToolError(f"extra command {key!r} is not a submit command name or +Attribute.")
        if _multiline(value):
            raise ToolError(f"{key} must be one line.")
    for item in [] if isinstance(queue, int) else queue:
        if _multiline(item):
            raise ToolError("Each queue item must be one line.")
        if not item.strip() or item.strip()[0] in ")#":
            raise ToolError(
                f"Queue item {item!r} is not accepted: an empty item or one starting with "
                "')' or '#' would end the item list early or be read as a comment."
            )
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
            f"The work directory {workdir} does not exist and the server could not create "
            "it at startup. Create it before submitting; the job's log, stdout and stderr go there."
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


def runs_commands(text: str) -> str | None:
    """Why the text is refused, or None. include lines and queue ... | statements
    make condor_submit read files or run commands as the server's user. Runs
    before any parse, since htcondor.Submit() itself processes include lines."""
    statements: list[str] = []
    for raw in CONTINUATION.sub("", text).splitlines():
        line = raw.strip()
        head = line.lower()
        if re.match(r"include[\s:]", head):
            return f"{line!r}: include lines are not accepted, since they read files or run commands at submit time."
        if re.match(r"queue\b", head):
            statements.append(line)
    if len(statements) > 1:  # the parser reports only the first
        return f"{statements[1]!r}: a second queue statement is not accepted, since only the first is checked."
    try:
        args = htcondor.Submit(text).getQArgs().strip()
    except (ValueError, htcondor.HTCondorException):
        return None  # parse() reports the error
    if "|" in args or "$" in args:  # macros expand after this check
        return f"'queue {args}': queue statements with a | or a macro are not accepted, since they can run a command at submit time."
    return None


def load(submit_text: str | None, submit_file: str | None) -> tuple[str, Path | None]:
    """The submit text and, for a file, its directory."""
    if submit_text is not None and submit_file is None:
        return submit_text, None
    if submit_file is None or submit_text is not None:
        raise ToolError("Give exactly one of submit_text and submit_file.")
    path = Path(os.path.expanduser(submit_file)).absolute()
    try:
        return path.read_text(), path.parent
    except OSError as exc:
        raise ToolError(f"Could not read submit file {path}: {exc.strerror or exc}.") from exc


def prepare(sub: htcondor.Submit, default_dir: Path) -> Path:
    """Pin down where the job runs from, the way condor_submit would if run in
    default_dir: initialdir defaults to it, a relative initialdir is taken
    against it, and so is a relative executable (not against initialdir), since
    the bindings would otherwise take it against the server's own cwd."""
    base = Path(os.path.expanduser(sub.get("initialdir") or "."))
    if not base.is_absolute():
        base = default_dir / base
    if "$(" not in str(base) and not base.is_dir():  # a macro path is checked by the dry run
        raise ToolError(f"initialdir {base} does not exist.")
    sub["initialdir"] = str(base)
    executable = os.path.expanduser(sub.get("executable") or "")
    if executable and "$(" not in executable and not os.path.isabs(executable):
        sub["executable"] = str(default_dir / executable)
    return base


def dry_run(text: str, cwd: Path | None = None) -> tuple[list[classad.ClassAd], list[str]]:
    """Job ads from condor_submit -dry-run plus its errors and warnings.
    Only the first ad is complete; the rest hold the attributes that differ.
    Relative paths in the text resolve against cwd, which defaults to an
    empty temp dir."""
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
                cwd=cwd or tmp, capture_output=True, text=True, timeout=60,
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


def complete_ads(ads: list[classad.ClassAd]) -> list[classad.ClassAd]:
    """One full ad per job: the dry run writes proc 0 whole and later procs as
    the attributes that differ from it."""
    full: list[classad.ClassAd] = []
    for ad in ads:
        if full:
            merged = classad.parseOne(str(full[0]))
            for key, value in ad.items():
                merged[key] = value
            ad = merged
        full.append(ad)
    return full


def dry_run_row(ad: classad.ClassAd) -> dict[str, Any]:
    """What one job would look like, from a dry-run ad (which has no real id)."""
    return {
        "proc": get(ad, "ProcId"),
        "command": classads.command_line(ad),
        "submit_dir": get(ad, "Iwd"),
        "requested": {
            "cpus": get(ad, "RequestCpus"),
            "memory_mb": get(ad, "RequestMemory"),
            "disk_kb": get(ad, "RequestDisk"),
            "gpus": get(ad, "RequestGPUs", 0),
        },
        "files": {
            "stdout": get(ad, "Out"),
            "stderr": get(ad, "Err"),
            "log": get(ad, "UserLog"),
            "transfer_input": classads.split_files(get(ad, "TransferInput")),
        },
        "requirements": str(ad.get("Requirements")),
    }


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
