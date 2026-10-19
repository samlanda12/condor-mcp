"""The MCP server: MCP tools over an HTCondor pool.
"""

from __future__ import annotations

from collections import Counter
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from htcondor2 import JobStatus

from condor_mcp import classads, pool
from condor_mcp.classads import get
from condor_mcp.pool import log, settings

mcp = FastMCP("condor-mcp")

READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}

JobStatusName = Literal[
    "idle", "running", "held", "completed", "removed", "suspended", "transferring_output"
]
OwnerField = Field(
    description="Whose jobs: 'me' for the user this server runs as, 'all' for everyone, "
    "or a username."
)


def _job_projection(base: list[str]) -> list[str]:
    return base + [a for a in settings.job_attrs if a not in base]


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def job_details(
    cluster_id: Annotated[int, Field(ge=1, description="Cluster number, the part before the dot in 123.0")],
    proc_id: Annotated[int, Field(ge=0, description="Process number within the cluster, the part after the dot")] = 0,
) -> dict[str, Any]:
    """Describe one HTCondor job: status, where it is running, resources requested
    vs used, input and output file locations, and, if it is held, the hold reason
    in plain language. Use this when a user asks about a specific job by ID.
    Only jobs still in the queue are visible; completed jobs that have left the
    queue are not returned."""
    log.info("job_details cluster_id=%s proc_id=%s", cluster_id, proc_id)
    found = pool.query_jobs(
        f"ClusterId == {cluster_id} && ProcId == {proc_id}",
        _job_projection(classads.JOB_PROJECTION),
        limit=1,
    )
    if not found:
        raise ToolError(
            f"Job {cluster_id}.{proc_id} is not in the queue. It may have finished "
            "and left the queue, or the ID may be wrong. Try list_jobs to see what "
            "is queued."
        )
    schedd, ad = found[0]
    summary = classads.summarize_job(ad, settings.job_attrs)
    if pool.multi_schedd():
        summary["schedd"] = schedd
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
    still in the queue are listed; jobs that completed and left the queue are not."""
    log.info("list_jobs owner=%s status=%s limit=%s", owner, status, limit)
    constraint, name = pool.owner_constraint(owner)
    counts = Counter(
        classads.JOB_STATUS.get(get(ad, "JobStatus"), "unknown")
        for _, ad in pool.query_jobs(constraint, ["JobStatus"])
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
        if pool.multi_schedd():
            row["schedd"] = schedd
        rows.append(row)
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
            f"No {what} in the queue {who}. Jobs that finished have left the queue."
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
        if pool.multi_schedd():
            row["schedd"] = schedd
        rows.append(row)
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
        raise ToolError(
            "The collector returned no slots. Either no execute machines are "
            "reporting or the slot filter (CONDOR_MCP_SLOT_CONSTRAINT = "
            f"{settings.slot_constraint!r}) excludes everything."
        )

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
        total_c = int(get(ad, "TotalSlotCpus", get(ad, "Cpus", 0)) or 0)
        total_m = int(get(ad, "TotalSlotMemory", get(ad, "Memory", 0)) or 0)
        total_g = int(get(ad, "TotalSlotGPUs", get(ad, "GPUs", 0)) or 0)
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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
