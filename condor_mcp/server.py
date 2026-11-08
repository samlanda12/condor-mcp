"""The MCP server: MCP tools over an HTCondor pool.
"""

from __future__ import annotations

import getpass
import os
from collections import Counter
from pathlib import Path
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

import classad2 as classad
import htcondor2 as htcondor
from htcondor2 import JobStatus

from condor_mcp import classads, pool, submit
from condor_mcp.classads import get
from condor_mcp.pool import log, settings

mcp = FastMCP("condor-mcp")

READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}
# Changes the queue; a client should confirm with the user before calling.
WRITE = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False}

JobStatusName = Literal[
    "idle", "running", "held", "completed", "removed", "suspended", "transferring_output"
]
OwnerField = Field(
    description="Whose jobs: 'me' for the user this server runs as, 'all' for everyone, "
    "or a username."
)


def _job_projection(base: list[str]) -> list[str]:
    return base + [a for a in settings.job_attrs if a not in base]


def _no_slots() -> str:
    return (
        "The collector returned no slots. Either no execute machines are "
        "reporting or the slot filter (CONDOR_MCP_SLOT_CONSTRAINT = "
        f"{settings.slot_constraint!r}) excludes everything."
    )


ClusterField = Field(ge=1, description="Cluster number, the part before the dot in 123.0")


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def job_details(
    cluster_id: Annotated[int, ClusterField],
    proc_id: Annotated[int, Field(ge=0, description="Process number within the cluster, the part after the dot")] = 0,
) -> dict[str, Any]:
    """Describe one HTCondor job: status, where it is running, resources requested
    vs used, input and output file locations, and, if it is held, the hold reason
    in plain language. Use this when a user asks about a specific job by ID.
    Only jobs still in the queue are visible; for a job that has completed or been
    removed and left the queue, use job_history."""
    log.info("job_details cluster_id=%s proc_id=%s", cluster_id, proc_id)
    found = pool.query_jobs(
        f"ClusterId == {cluster_id} && ProcId == {proc_id}",
        _job_projection(classads.JOB_PROJECTION),
        limit=1,
    )
    if not found:
        raise ToolError(
            f"Job {cluster_id}.{proc_id} is not in the queue. It may have finished "
            f"and left the queue (job_history with cluster_id={cluster_id} shows how it "
            "ended), or the ID may be wrong. Try list_jobs to see what is queued."
        )
    schedd, ad = found[0]
    summary = pool.tag_schedd(classads.summarize_job(ad, settings.job_attrs), schedd)
    log.info("  -> job %s is %s", summary["job_id"], summary["status"])
    return summary


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def list_jobs(
    owner: Annotated[str, OwnerField] = "me",
    status: Annotated[
        JobStatusName | None,
        Field(description="Only jobs in this state. Omit for every state."),
    ] = None,
    limit: Annotated[int, Field(ge=1, le=500, description="Most jobs to list.")] = 50,
) -> dict[str, Any]:
    """List jobs in the queue, one compact line each, plus a count of the owner's
    jobs by status. Use this for "what's running", "how many jobs do I have", or
    to find a job ID; use job_details for the full picture of one job. Only jobs
    still in the queue are listed; job_history lists jobs that have left it."""
    log.info("list_jobs owner=%s status=%s limit=%s", owner, status, limit)
    constraint, name = pool.owner_constraint(owner)
    counts = Counter(
        classads.status_name(ad) for _, ad in pool.query_jobs(constraint, ["JobStatus"])
    )
    if status:
        constraint += f" && JobStatus == {classads.STATUS_CODE[status]}"
    found = pool.query_jobs(constraint, _job_projection(classads.JOB_LIST_PROJECTION), limit=limit)
    found.sort(key=lambda pair: (get(pair[1], "ClusterId", 0), get(pair[1], "ProcId", 0)))
    rows = []
    for schedd, ad in found[:limit]:
        row = classads.compact_job(ad)
        if settings.job_attrs:
            row["site"] = classads.site_attrs(ad, settings.job_attrs)
        rows.append(pool.tag_schedd(row, schedd))
    matching = counts[status] if status else sum(counts.values())
    out = {
        "owner": name or "all",
        "status_filter": status,
        "counts_by_status": dict(counts),
        "jobs": rows,
        "truncated": matching > len(rows),
    }
    if not rows:
        who = f"for {name}" if name else "for anyone"
        what = f"{status} jobs" if status else "jobs"
        out["note"] = (
            f"No {what} in the queue {who}. Jobs that finished have left the queue; "
            "job_history lists them."
        )
    log.info("  -> %d of %d jobs", len(rows), matching)
    return out


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def hold_reasons(owner: Annotated[str, OwnerField] = "me") -> dict[str, Any]:
    """Explain why jobs are on hold: every held job with HTCondor's hold message,
    the numeric hold code, and what that code means in plain language, plus a
    tally by code. Use this for "why is my job held" or "what's wrong with my
    jobs". A job that is not held will not appear; check it with job_details."""
    log.info("hold_reasons owner=%s", owner)
    constraint, name = pool.owner_constraint(owner)
    found = pool.query_jobs(
        f"({constraint}) && JobStatus == {int(JobStatus.HELD)}",
        _job_projection(classads.HELD_PROJECTION),
    )
    found.sort(key=lambda pair: (get(pair[1], "ClusterId", 0), get(pair[1], "ProcId", 0)))
    rows = []
    for schedd, ad in found:
        row = classads.held_job(ad)
        if settings.job_attrs:
            row["site"] = classads.site_attrs(ad, settings.job_attrs)
        rows.append(pool.tag_schedd(row, schedd))
    tally = Counter(row["code"] for row in rows)
    out = {
        "owner": name or "all",
        "count": len(rows),
        "by_code": [
            {"code": code, "count": n, "meaning": classads.HOLD_CODES.get(code, "see the HTCondor manual")}
            for code, n in sorted(tally.items(), key=lambda kv: -kv[1])
        ],
        "held_jobs": rows,
    }
    if not rows:
        out["note"] = f"No held jobs for {name or 'anyone'}."
    log.info("  -> %d held", len(rows))
    return out


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def job_history(
    owner: Annotated[str, OwnerField] = "me",
    cluster_id: Annotated[
        int | None, Field(ge=1, description="Only this cluster. Omit for the owner's most recent jobs.")
    ] = None,
    limit: Annotated[int, Field(ge=1, le=200, description="Most jobs to list, newest first.")] = 20,
) -> dict[str, Any]:
    """List jobs that have left the queue, newest first: completed ones with their
    exit code, removed ones with the remove reason. Each row also gives wall time,
    memory and disk used versus requested, and the machine the job ran on. Use this
    for "did my job finish", "why did it fail", "how much memory did it use", or a
    job that job_details no longer finds. Jobs still in the queue are not listed;
    use list_jobs or job_details for those."""
    log.info("job_history owner=%s cluster_id=%s limit=%s", owner, cluster_id, limit)
    constraint, name = pool.owner_constraint(owner)
    if cluster_id is not None:
        constraint = f"({constraint}) && ClusterId == {cluster_id}"
    # One extra per schedd, so a list cut at the limit can say so
    found = pool.query_history(constraint, _job_projection(classads.HISTORY_PROJECTION), limit + 1)
    rows = []
    for schedd, ad in found[:limit]:
        row = classads.finished_job(ad)
        if settings.job_attrs:
            row["site"] = classads.site_attrs(ad, settings.job_attrs)
        rows.append(pool.tag_schedd(row, schedd))
    out: dict[str, Any] = {"owner": name or "all", "jobs": rows, "truncated": len(found) > limit}
    if not rows:
        who = f"for {name}" if name else "for anyone"
        if cluster_id is None:
            out["note"] = f"No finished jobs in the history {who}."
        else:
            someone_else = " the job may belong to someone else (try owner='all')," if name else ""
            out["note"] = (
                f"Cluster {cluster_id} is not in the history {who}. If it is still queued, "
                f"job_details shows it. Otherwise the ID may be wrong,{someone_else} or it is "
                "old enough that the schedd has dropped it from its history."
            )
    log.info("  -> %d finished jobs", len(rows))
    return out


OutputKind = Literal["stdout", "stderr", "log"]
# Submit command that names the file of each kind of output
_SUBMIT_COMMANDS = {"stdout": "output", "stderr": "error", "log": "log"}
# The most of a file job_output returns, however many lines were asked for
MAX_OUTPUT_BYTES = 64 * 1024


def _tail(path: Path, lines: int) -> tuple[str, int, bool]:
    """The last `lines` lines of a file, read from the end and capped at MAX_OUTPUT_BYTES.

    Returns (text, file size in bytes, whether anything before the text was left out)."""
    with path.open("rb") as f:
        size = f.seek(0, os.SEEK_END)
        start = max(0, size - MAX_OUTPUT_BYTES)
        f.seek(max(0, start - 1))  # one byte early, to see whether the cut is at a line start
        data = f.read()
    mid_line = bool(start) and data[:1] != b"\n"
    if start:
        data = data[1:]
    parts = data.decode("utf-8", errors="replace").splitlines()
    if mid_line and len(parts) > 1:
        parts = parts[1:]  # the cap landed mid-line
    return "\n".join(parts[-lines:]), size, bool(start) or len(parts) > lines


def _pending_output_note(ad: classad.ClassAd, which: str, in_queue: bool) -> str | None:
    """Why a job's stdout or stderr is missing or empty on the submit host, if its state
    explains it. None for the log, which the schedd writes at submit."""
    if which == "log":
        return None
    # A completed or removed job can stay in the queue (leave_in_queue), but it has ended
    active = in_queue and get(ad, "JobStatus") not in (JobStatus.COMPLETED, JobStatus.REMOVED)
    if not get(ad, "NumJobStarts", 0):
        if active:
            return f"No {which} yet: the job is {classads.status_name(ad)} and has not started."
        return f"No {which}: the job never started, so it wrote none."
    streamed = get(ad, "StreamOut" if which == "stdout" else "StreamErr", False)
    if active and get(ad, "ShouldTransferFiles", "YES") != "NO" and not streamed:
        return (
            f"No {which} yet: the job is {classads.status_name(ad)}, and with file transfer "
            f"HTCondor copies its {which} back to the submit host only when the job ends. "
            "which='log' shows its progress meanwhile."
        )
    return None


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def job_output(
    cluster_id: Annotated[int, ClusterField],
    proc_id: Annotated[int, Field(ge=0, description="Process number within the cluster, the part after the dot")] = 0,
    which: Annotated[
        OutputKind,
        Field(description="stdout or stderr: what the program printed. log: HTCondor's event "
              "log for the job (submitted, started where, held, evicted, exit code)."),
    ] = "stdout",
    tail_lines: Annotated[int, Field(ge=1, le=500, description="How many lines from the end.")] = 50,
) -> dict[str, Any]:
    """Show the last lines of a job's stdout, stderr, or HTCondor event log, read
    from the submit host. Works for jobs in the queue and for jobs that have left
    it, but only for jobs the configured user owns. Use this for "what did my job
    print", "why did it crash", or "what happened to it". With file transfer, a
    running job's stdout and stderr reach the submit host only when it ends;
    until then the log shows its progress."""
    log.info("job_output cluster_id=%s proc_id=%s which=%s tail_lines=%s", cluster_id, proc_id, which, tail_lines)
    found = pool.find_job(cluster_id, proc_id, classads.OUTPUT_PROJECTION)
    if found is None:
        raise ToolError(
            f"Job {cluster_id}.{proc_id} is in neither the queue nor the schedd history. The ID "
            "may be wrong, or the job is old enough that the schedd has dropped it from its history."
        )
    schedd, ad, in_queue = found

    if get(ad, "Owner") != settings.user:
        raise ToolError(
            f"Refusing to read the output of job {cluster_id}.{proc_id}: it belongs to "
            f"{get(ad, 'Owner') or '<no owner>'}, not {settings.user}. This server only reads "
            "output of jobs owned by the user it runs as (CONDOR_MCP_USER)."
        )
    attr, command = classads.OUTPUT_ATTRS[which], _SUBMIT_COMMANDS[which]
    out: dict[str, Any] = {
        "job_id": classads.job_id(ad), "status": classads.status_name(ad), "in_queue": in_queue, "which": which,
    }
    pool.tag_schedd(out, schedd)
    name = get(ad, attr)
    if not name or name == "/dev/null":
        out["note"] = (
            f"The job keeps no {which}: its submit file sets no `{command}` file, or sets it to "
            f"/dev/null. Add `{command} = <file>` to keep it next time."
        )
        log.info("  -> no %s file", which)
        return out
    iwd = get(ad, "Iwd")
    if not iwd and not Path(str(name)).is_absolute():
        out["note"] = (
            f"The job's {which} file is {name}, a relative path, but the job ad records no "
            "submit directory (Iwd) to resolve it against, so it cannot be located."
        )
        log.info("  -> no Iwd for %s", name)
        return out
    path = Path(iwd or "") / str(name)  # an absolute name replaces Iwd
    out["path"] = str(path)
    try:
        text, size, truncated = _tail(path, tail_lines)
    except FileNotFoundError:
        out["text"] = ""
        out["note"] = _pending_output_note(ad, which, in_queue) or (
            f"{path} does not exist. It may have been moved or deleted since the job wrote it."
        )
        log.info("  -> %s missing", path)
        return out
    except OSError as exc:  # permissions, a directory, ...
        raise ToolError(f"Could not read the job's {which} at {path}: {exc}") from exc
    out.update(text=text, size_bytes=size, truncated=truncated)
    if not size:
        # condor_submit creates stdout and stderr empty at submit, so empty can mean pending
        out["note"] = _pending_output_note(ad, which, in_queue) or f"{path} is empty."
    log.info("  -> %d bytes of %s", len(text), path)
    return out


@mcp.tool(annotations=READ_ONLY, tags={"pool", "read-only"})
def pool_status(
    detail: Annotated[bool, Field(description="Also return one row per slot.")] = False,
) -> dict[str, Any]:
    """Report the pool's capacity from the collector: machines, slots by state,
    CPUs, memory and GPUs in total and currently available, GPUs by model where
    the pool reports one, and the largest block of free CPUs on any one machine.
    Use this for "is the cluster busy", "are there free GPUs", or "why is my job
    still idle". Slot counts include dynamic slots, so a machine with a
    partitionable slot shows one unclaimed slot holding its spare resources plus
    one claimed slot per running job. Which slots count is set by the server's
    slot filter."""
    log.info("pool_status detail=%s", detail)
    projection = classads.SLOT_PROJECTION + [
        a for a in settings.slot_attrs if a not in classads.SLOT_PROJECTION
    ]
    ads = pool.query_slots(projection)
    if not ads:
        raise ToolError(_no_slots())

    by_state: Counter[str] = Counter()
    machines: set[str] = set()
    cpus = {"total": 0, "available": 0}
    memory = {"total": 0, "available": 0}
    gpus = {"total": 0, "available": 0}
    gpu_models: dict[str, dict[str, int]] = {}
    largest: dict[str, Any] | None = None

    for ad in ads:
        machines.add(get(ad, "Machine") or get(ad, "Name"))
        by_state[get(ad, "State", "Unknown")] += 1
        if get(ad, "DynamicSlot", False):
            continue  # its resources are accounted for through the parent slot
        unclaimed = get(ad, "State") == "Unclaimed"
        partitionable = bool(get(ad, "PartitionableSlot", False))
        totals = classads.slot_totals(ad)
        total_c, total_m, total_g = totals["Cpus"], totals["Memory"], totals["GPUs"]
        # A partitionable slot's own Cpus/Memory/GPUs are what is left after its
        # dynamic children took theirs. A static slot is all-or-nothing.
        free_c = int(get(ad, "Cpus", 0) or 0) if (partitionable or unclaimed) else 0
        free_m = int(get(ad, "Memory", 0) or 0) if (partitionable or unclaimed) else 0
        free_g = int(get(ad, "GPUs", 0) or 0) if (partitionable or unclaimed) else 0
        cpus["total"] += total_c
        cpus["available"] += free_c
        memory["total"] += total_m
        memory["available"] += free_m
        gpus["total"] += total_g
        gpus["available"] += free_g
        if total_g:
            model = get(ad, "CUDADeviceName") or "unknown model"
            entry = gpu_models.setdefault(model, {"total": 0, "available": 0})
            entry["total"] += total_g
            entry["available"] += free_g
        if largest is None or free_c > largest["free_cpus"]:
            largest = {"machine": get(ad, "Machine"), "free_cpus": free_c, "machine_cpus": total_c}

    out = {
        "collector": settings.collector or "from HTCondor config",
        "slot_filter": settings.slot_constraint,
        "machines": len(machines),
        "slots": {"total": len(ads), "by_state": dict(by_state)},
        "cpus": cpus,
        "memory_mb": memory,
        "gpus": {**gpus, "by_model": gpu_models},
        "largest_free_cpu_block": largest,
    }
    if detail:
        out["slot_list"] = [classads.summarize_slot(ad, settings.slot_attrs) for ad in ads]
    log.info("  -> %d slots, %d/%d cpus free", len(ads), cpus["available"], cpus["total"])
    return out


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def draft_submit(
    executable: Annotated[str, Field(description="Program or script to run.")],
    arguments: Annotated[str, Field(description="Its command-line arguments. $(item) is the queue item, $(ProcId) the job number.")] = "",
    input_files: Annotated[list[str] | None, Field(description="Files to copy to the execute machine before the job starts. $(item) stands for the queue item.")] = None,
    output_files: Annotated[list[str] | None, Field(description="Files or directories to copy back when the job ends. Empty means every new file in the job's scratch directory.")] = None,
    cpus: Annotated[int, Field(ge=1)] = 1,
    memory_mb: Annotated[int, Field(ge=1)] = 1024,
    disk_mb: Annotated[int | None, Field(ge=1, description="Scratch disk. Omit to let HTCondor estimate it.")] = None,
    gpus: Annotated[int, Field(ge=0)] = 0,
    queue: Annotated[int | list[str], Field(description="How many copies to queue, or a list of items to queue one job each for.")] = 1,
    extra: Annotated[dict[str, str] | None, Field(description="Other submit commands, verbatim, such as {\"requirements\": \"HasAVX2\", \"+WantCloudBurst\": \"\\\"orangegrid-2026\\\"\"}. They come last and override the generated commands.")] = None,
) -> dict[str, Any]:
    """Turn a structured job description into an HTCondor submit file and check
    it without queuing anything: the text is parsed, run through condor_submit's
    dry run, and its requirements are matched against the pool's slots at full
    size. Returns the submit text to show the user, how many jobs it would
    queue, and warnings: a missing executable or input file, a typo in an extra
    command, or a request no machine in the pool could ever satisfy. Use
    queue=["a.csv", "b.csv"] with $(item) in arguments or input_files to run
    one job per item. The job runs from the server's work directory; its log,
    stdout and stderr land there named after the job id, and relative paths
    are taken relative to it."""
    log.info("draft_submit executable=%s queue=%s", executable, queue)
    workdir = settings.workdir
    executable = submit.resolve(executable, workdir)
    input_files = [submit.resolve(f, workdir) for f in input_files or []]
    text = submit.compose(
        executable=executable, arguments=arguments, input_files=input_files,
        output_files=output_files or [], cpus=cpus, memory_mb=memory_mb, disk_mb=disk_mb,
        gpus=gpus, queue=queue, extra=extra or {}, workdir=workdir,
    )
    if reason := submit.runs_commands(text):
        raise ToolError(f"The draft is not accepted: {reason}")
    warnings = submit.check_files(executable, input_files, queue, workdir)
    out: dict[str, Any] = {
        "submit_text": text,
        "work_dir": str(workdir),
        "jobs": 0,
        "submittable": False,
        "requirements": None,
        "slots": None,
        "warnings": warnings,
    }
    if problem := submit.parse(text):
        warnings.append(problem)
        return out
    ads, messages = submit.dry_run(text)
    warnings.extend(messages)
    if not ads:
        return out
    out["jobs"] = len(ads)
    out["submittable"] = True
    out["requirements"] = str(ads[0].get("Requirements"))
    try:
        slots = submit.slot_check(ads[0])
    except ToolError as exc:
        warnings.append(f"Could not check the requirements against the pool: {exc}")
        return out
    out["slots"] = slots
    big = slots["largest_slot"]
    if slots["slots"] == 0:
        warnings.append(f"Could not check the requirements against the pool: {_no_slots()}")
    elif slots["fit_requirements"] == 0:
        ad = ads[0]  # the effective requests, since extra can override the arguments
        warnings.append(
            "No slot in the pool satisfies the job's requirements, even when empty. The "
            f"job asks for {get(ad, 'RequestCpus')} CPUs, {get(ad, 'RequestMemory')} MB and "
            f"{get(ad, 'RequestGPUs', 0)} GPUs; no slot has more than {big['cpus']} CPUs, "
            f"{big['memory_mb']} MB or {big['gpus']} GPUs. Check the requirements line too."
        )
    elif slots["would_accept"] == 0:
        warnings.append(
            f"{slots['fit_requirements']} slots satisfy the job's requirements but none "
            "would accept it under its own START policy, likely because the dry-run ad has no Owner."
        )
    log.info("  -> %d jobs, %d warnings", out["jobs"], len(warnings))
    return out


@mcp.tool(annotations=WRITE, tags={"jobs", "write"})
def submit_job(
    submit_text: Annotated[str | None, Field(description="A complete submit description, such as the submit_text from draft_submit once the user has approved it.")] = None,
    submit_file: Annotated[str | None, Field(description="Instead of text: the path of a submit file on the submit host. Its relative paths are taken relative to its own directory.")] = None,
    dry_run: Annotated[bool, Field(description="Queue nothing; return the job ads HTCondor would create.")] = False,
) -> dict[str, Any]:
    """Queue jobs from a submit description, given as text or as the path of a
    submit file on the submit host. Jobs run as the user this server runs as,
    and go to the first schedd in CONDOR_MCP_SCHEDDS, or the local one if unset.
    The description first goes through condor_submit's dry run and is refused
    if that reports an error. With dry_run=True nothing is queued and the
    generated job ads come back instead, one row per job. include lines, a
    second queue statement, and queue statements with a | or a macro are
    refused. Returns the cluster id, the number of jobs, and one list_jobs row
    per job; follow a job with job_details."""
    log.info("submit_job file=%s dry_run=%s text=%s", submit_file, dry_run, submit_text is not None)
    text, file_dir = submit.load(submit_text, submit_file)
    if reason := submit.runs_commands(text):
        raise ToolError(f"Refusing to submit: {reason}")
    if problem := submit.parse(text):
        raise ToolError(problem)
    description = htcondor.Submit(text)
    if file_dir is None:
        try:
            settings.workdir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ToolError(f"Could not create the work directory {settings.workdir}: {exc}") from exc
    default_dir = file_dir or settings.workdir
    base = submit.prepare(description, default_dir)
    # The original text still has a relative initialdir, so run from where condor_submit would.
    ads, warnings = submit.dry_run(text, cwd=default_dir)
    errors = [m for m in warnings if m.startswith("ERROR")]
    if errors or not ads:
        reported = " ".join(errors or warnings) or "no job ads"
        raise ToolError(f"Refusing to submit, condor_submit reported: {reported}")
    if dry_run:
        rows = [submit.dry_run_row(ad) for ad in submit.complete_ads(ads)]
        log.info("  -> dry run, %d jobs", len(rows))
        return {"dry_run": True, "jobs": len(rows), "submit_dir": str(base), "job_ads": rows, "warnings": warnings}

    name, schedd = next(pool.schedds())
    try:
        result = schedd.submit(description)
    except (htcondor.HTCondorException, RuntimeError) as exc:
        raise ToolError(f"The schedd ({name}) refused the submit: {exc}") from exc
    cluster = result.cluster()
    try:
        queued = schedd.query(f"ClusterId == {cluster}", projection=classads.JOB_LIST_PROJECTION)
    except htcondor.HTCondorException as exc:
        raise ToolError(f"Submitted cluster {cluster} but could not read it back: {exc}") from exc
    queued.sort(key=lambda ad: get(ad, "ProcId", 0))
    out: dict[str, Any] = {
        "cluster_id": cluster,
        "jobs": result.num_procs(),
        "submit_dir": str(base),
        "job_list": [classads.compact_job(ad) for ad in queued],
        "warnings": warnings,
    }
    pool.tag_schedd(out, name)
    log.info("  -> cluster %d, %d jobs", cluster, result.num_procs())
    return out


# Changes a job's state. Repeating the call leaves the job as it is, so idempotent.
ACT = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True}
# Takes a job out of the queue for good; a client should warn the user before calling.
DESTRUCTIVE = {**ACT, "destructiveHint": True}

ProcField = Field(ge=0, description="Process number within the cluster. Omit to act on every job in the cluster.")
RemoveProcField = Field(
    ge=0, description="Process number within the cluster. Give this, or all_procs=True, never both.",
)
AllProcsField = Field(description="Remove every job in the cluster. Only valid without proc_id.")


def _reason_field(recorded: str) -> Any:
    return Field(min_length=1, max_length=200, description=f"Why, in a sentence. HTCondor records it {recorded}.")


HoldReasonField = _reason_field("on the job as the hold reason, and job_details shows it")
ReleaseReasonField = _reason_field("on the job as the release reason, and job_details shows it")
RemoveReasonField = _reason_field("in the job history as the remove reason; the job itself leaves the queue")
_ACT_PROJECTION = ["ClusterId", "ProcId", "Owner", "JobStatus", "HoldReason"]


def _act(
    action: htcondor.JobAction, cluster_id: int, proc_id: int | None, reason: str, missing_ok: bool = False
) -> dict[str, Any]:
    """Apply one JobAction to a cluster or a single job after checking it belongs
    to the configured user, and report each job's status before and after.
    With missing_ok, a job that is not in the queue is reported rather than an error."""
    verb = action.name.lower()
    constraint = f"ClusterId == {cluster_id}"
    label = f"cluster {cluster_id}"
    if proc_id is not None:
        constraint += f" && ProcId == {proc_id}"
        label = f"job {cluster_id}.{proc_id}"
    log.info("%s_job %s reason=%r", verb, label, reason)
    title = label.capitalize()
    # Not pool.query_jobs: we keep each Schedd so the action can reuse the one holding the job
    by_name: dict[str, htcondor.Schedd] = {}
    found: list[tuple[str, Any]] = []
    for name, schedd in pool.schedds():
        by_name[name] = schedd
        found.extend((name, ad) for ad in pool.query_schedd(name, schedd, constraint, _ACT_PROJECTION))
    if not found and missing_ok:
        log.info("  -> nothing to %s", verb)
        return {
            "action": verb, "reason": reason, "changed": 0, "jobs": [],
            "note": f"Nothing to {verb}: {label} is not in the queue. It may already have been "
            "removed or finished, or the ID may be wrong.",
        }
    if not found:
        raise ToolError(
            f"{title} is not in the queue. It may have finished and "
            "left the queue, or the ID may be wrong. Try list_jobs to see what is queued."
        )
    names = sorted({name for name, _ in found})
    if len(names) > 1:
        raise ToolError(
            f"{title} exists on more than one schedd ({', '.join(names)}), "
            "so the ID is ambiguous. Set CONDOR_MCP_SCHEDDS to the one that holds the job."
        )
    others = sorted({get(ad, "Owner") or "<no owner>" for _, ad in found} - {settings.user})
    if others:
        raise ToolError(
            f"Refusing to {verb} {label}: it belongs to {', '.join(others)}, not {settings.user}. "
            "This server only acts on jobs owned by the user it runs as (CONDOR_MCP_USER)."
        )
    (name,) = names
    schedd = by_name[name]
    before = {classads.job_id(ad): ad for _, ad in found}
    mine, _ = pool.owner_constraint("me")  # so the schedd enforces the owner check too
    try:
        # htcondor2 drops a plain-string reason on the floor; only the (text, code) tuple reaches
        # the schedd. A None code keeps HoldReasonCode at 1, the same as condor_hold.
        result = schedd.act(action, f"{constraint} && {mine}", reason=(reason, None))
    except (htcondor.HTCondorException, RuntimeError) as exc:
        raise ToolError(f"The schedd ({name}) could not {verb} {label}: {exc}") from exc
    if result is None:  # htcondor2 returns None instead of raising when the schedd sends no result
        raise ToolError(f"The schedd ({name}) returned no result for {verb} {label}; try again.")
    changed = get(result, "TotalChangedAds", 0)
    if changed == 0 and get(result, "TotalPermissionDenied", 0):
        # Names the OS user, not CONDOR_MCP_USER as the refusal above does
        raise ToolError(
            f"The schedd ({name}) denied permission to {verb} {label}. The server runs as "
            f"{getpass.getuser()}; only the job's owner or a queue superuser can change it."
        )
    after = {
        classads.job_id(ad): ad
        for ad in pool.query_schedd(name, schedd, constraint, _ACT_PROJECTION)
    }
    # Only a remove can take credit for a job that is gone
    gone = "removed" if action == htcondor.JobAction.Remove else "left the queue"
    rows = []
    for jid, old in before.items():
        new = after.get(jid)
        row = {
            "job_id": jid,
            "status_before": classads.status_name(old),
            "status_after": classads.status_name(new) if new else gone,
        }
        if new is None:
            row["note"] = "left the queue"
        elif get(new, "JobStatus") == JobStatus.HELD:
            row["hold_reason"] = get(new, "HoldReason")
        rows.append(row)
    out: dict[str, Any] = {"action": verb, "reason": reason, "changed": changed, "jobs": rows}
    if changed == 0:
        states = ", ".join(f"{r['job_id']} is {r['status_after']}" for r in rows)
        out["note"] = f"Nothing to {verb}: {states}."
    pool.tag_schedd(out, name)
    log.info("  -> %s changed %d of %d", verb, changed, len(rows))
    return out


@mcp.tool(annotations=ACT, tags={"jobs", "write"})
def hold_job(
    cluster_id: Annotated[int, ClusterField],
    reason: Annotated[str, HoldReasonField],
    proc_id: Annotated[int | None, ProcField] = None,
) -> dict[str, Any]:
    """Put a job on hold: a running job is stopped and an idle one stops waiting,
    and it stays in the queue until released. Use this to pause a job or to stop
    one that is running wrongly without losing it. Acts on every job in the
    cluster unless proc_id is given, and only on jobs the configured user owns.
    Returns each job's status before and after; a job that was already held is
    left alone and reported."""
    return _act(htcondor.JobAction.Hold, cluster_id, proc_id, reason)


@mcp.tool(annotations=ACT, tags={"jobs", "write"})
def release_job(
    cluster_id: Annotated[int, ClusterField],
    reason: Annotated[str, ReleaseReasonField],
    proc_id: Annotated[int | None, ProcField] = None,
) -> dict[str, Any]:
    """Release a held job so it goes back to idle and can be matched again. Fix
    the cause first (hold_reasons says what it was): a job released with the
    cause still present goes straight back on hold. Acts on every job in the
    cluster unless proc_id is given, and only on jobs the configured user owns.
    Returns each job's status before and after; a job that was not held is left
    alone and reported."""
    return _act(htcondor.JobAction.Release, cluster_id, proc_id, reason)


@mcp.tool(annotations=DESTRUCTIVE, tags={"jobs", "write"})
def remove_job(
    cluster_id: Annotated[int, ClusterField],
    reason: Annotated[str, RemoveReasonField],
    proc_id: Annotated[int | None, RemoveProcField] = None,
    all_procs: Annotated[bool, AllProcsField] = False,
) -> dict[str, Any]:
    """Remove a job from the queue for good: a running job is killed and an idle
    or held one is dropped. There is no undo; the job has to be submitted again.
    Output files already written stay where they are. Use this when a user wants
    to cancel, kill, or delete a job; use hold_job to pause one instead. Removes
    the one job proc_id names, or every job in the cluster with all_procs=True;
    one of the two is required. Acts only on jobs the configured user owns.
    Returns each job's status before and after; a job that has already left the
    queue is reported as such rather than an error."""
    # Unlike hold and release, an omitted proc_id does not mean the whole cluster: there is no
    # undo, so the caller has to say which it meant.
    if (proc_id is None) == (not all_procs):
        raise ToolError(
            f"Say what to remove from cluster {cluster_id}: proc_id for one job, or "
            "all_procs=True for every job in the cluster. Not both, and not neither."
        )
    # Removing twice is a no-op: the queue is already in the asked-for state.
    return _act(htcondor.JobAction.Remove, cluster_id, proc_id, reason, missing_ok=True)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
