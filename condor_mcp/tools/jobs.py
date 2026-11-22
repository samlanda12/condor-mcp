"""Tools that read jobs: in the queue, in the history, and their output files."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path
from typing import Annotated, Any, Literal

import classad2 as classad
from fastmcp.exceptions import ToolError
from htcondor2 import JobStatus
from pydantic import Field

from condor_mcp import classads, pool
from condor_mcp.app import READ_ONLY, ClusterField, OwnerField, job_projection, mcp, require_job
from condor_mcp.classads import get
from condor_mcp.pool import log, settings

JobStatusName = Literal["idle", "running", "held", "completed", "removed", "suspended", "transferring_output"]


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def job_details(
    cluster_id: Annotated[int, ClusterField],
    proc_id: Annotated[int, Field(ge=0, description="Process number within the cluster, the part after the dot")] = 0,
) -> dict[str, Any]:
    """Describe one HTCondor job, queued or finished: status, where it is running or
    ran, resources requested vs used, input and output file locations, the hold
    reason in plain language if it is held, and the exit code or remove reason once
    it has ended. Use this when a user asks about a specific job by ID. A job that
    has left the queue is read from the schedd history, and in_queue is false."""
    log.info("job_details cluster_id=%s proc_id=%s", cluster_id, proc_id)
    schedd, ad, in_queue, warnings = require_job(cluster_id, proc_id, job_projection(classads.JOB_PROJECTION))
    summary = classads.summarize_job(ad, settings.job_attrs)
    summary["in_queue"] = in_queue
    if warnings:
        summary["warnings"] = warnings
    pool.tag_schedd(summary, schedd)
    log.info("  -> job %s is %s, in_queue=%s", summary["job_id"], summary["status"], in_queue)
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
    counted, failed = pool.query_jobs(constraint, ["JobStatus"])
    counts = Counter(classads.status_name(ad) for _, ad in counted)
    if status:
        constraint += f" && JobStatus == {classads.STATUS_CODE[status]}"
    found, failed = pool.query_jobs(
        constraint, job_projection(classads.JOB_LIST_PROJECTION), limit=limit, failed=failed
    )
    found.sort(key=lambda pair: (get(pair[1], "ClusterId", 0), get(pair[1], "ProcId", 0)))
    rows = []
    for schedd, ad in found[:limit]:
        row = classads.compact_job(ad)
        if settings.job_attrs:
            row["site"] = classads.site_attrs(ad, settings.job_attrs)
        rows.append(pool.tag_schedd(row, schedd))
    matching = counts[status] if status else sum(counts.values())
    out: dict[str, Any] = {
        "owner": name or "all",
        "status_filter": status,
        "counts_by_status": dict(counts),
        "jobs": rows,
        "truncated": matching > len(rows),
    }
    if not rows:
        who = f"for {name}" if name else "for anyone"
        what = f"{status} jobs" if status else "jobs"
        out["note"] = f"No {what} in the queue {who}. Jobs that finished have left the queue; job_history lists them."
    if failed:
        out["warnings"] = list(failed.values())
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
    found, failed = pool.query_jobs(
        f"({constraint}) && JobStatus == {int(JobStatus.HELD)}",
        job_projection(classads.HELD_PROJECTION),
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
    if failed:
        out["warnings"] = list(failed.values())
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
    exit code, removed ones with the remove reason. Each row also gives memory and
    disk used versus requested and, for jobs that started, wall time and the machine
    they ran on. Use this for "did my job finish", "why did it fail", or "how much
    memory did it use" across recent jobs. Jobs still in the queue are not listed; use
    list_jobs for those. job_details covers one job by ID whether queued or finished."""
    log.info("job_history owner=%s cluster_id=%s limit=%s", owner, cluster_id, limit)
    constraint, name = pool.owner_constraint(owner)
    if cluster_id is not None:
        constraint = f"({constraint}) && ClusterId == {cluster_id}"
    # One extra per schedd, so a list cut at the limit can say so
    found, failed = pool.query_history(constraint, job_projection(classads.HISTORY_PROJECTION), limit + 1)
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
    if failed:
        out["warnings"] = list(failed.values())
    log.info("  -> %d finished jobs", len(rows))
    return out


OutputKind = Literal["stdout", "stderr", "log"]
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
        Field(
            description="stdout or stderr: what the program printed. log: HTCondor's event "
            "log for the job (submitted, started where, held, evicted, exit code)."
        ),
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
    schedd, ad, in_queue, warnings = require_job(cluster_id, proc_id, classads.OUTPUT_PROJECTION)

    if get(ad, "Owner") != settings.user:
        raise ToolError(
            f"Refusing to read the output of job {cluster_id}.{proc_id}: it belongs to "
            f"{get(ad, 'Owner') or '<no owner>'}, not {settings.user}. This server only reads "
            "output of jobs owned by the user it runs as (CONDOR_MCP_USER)."
        )
    attr, command = classads.OUTPUT_FILES[which]
    out: dict[str, Any] = {
        "job_id": classads.job_id(ad),
        "status": classads.status_name(ad),
        "in_queue": in_queue,
        "which": which,
    }
    pool.tag_schedd(out, schedd)
    if warnings:
        out["warnings"] = warnings
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
