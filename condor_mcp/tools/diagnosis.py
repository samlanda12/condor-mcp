"""The why_idle tool."""

from __future__ import annotations

from typing import Annotated, Any

import classad2 as classad
from fastmcp.exceptions import ToolError
from htcondor2 import JobStatus
from pydantic import Field

from condor_mcp import classads, diagnose, pool
from condor_mcp.app import READ_ONLY, ClusterField, mcp, no_slots, require_job
from condor_mcp.classads import get
from condor_mcp.pool import log
from condor_mcp.tools.pool import priority_row, priority_table, submitter_name


def _not_idle(job: classad.ClassAd, in_queue: bool) -> dict[str, Any]:
    """What why_idle says instead for a job that is not idle."""
    status = classads.status_name(job)
    if not in_queue:
        return {"note": f"The job has left the queue ({status}). job_details shows how it ended."}
    if status == "held":
        return {
            "note": "The job is held, not idle, so it will not run until it is released. Fix the "
            "hold's cause first, or it goes straight back on hold.",
            "hold": classads.hold_info(job),
        }
    if status == "running":
        return {"note": f"The job is running, on {get(job, 'RemoteHost')}."}
    return {"note": f"The job is {status}, not idle."}


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def why_idle(
    cluster_id: Annotated[int, ClusterField],
    proc_id: Annotated[int, Field(ge=0, description="Process number within the cluster, the part after the dot")] = 0,
) -> dict[str, Any]:
    """Explain why an idle job has not started, as condor_q -better-analyze does. The
    job is matched against every slot the server's slot filter lets through, both at
    full size and as each slot is now. reasons gives the likely causes in plain words,
    most likely first. The details: how many slots meet the job's requirements, how many
    of those accept it (their START policy and resource limits), how many could start it
    now; how many slots each requirement clause rules out; what the job asks for against
    the largest slot; GPUs that fit but sit on machines with too few CPUs or too little
    memory left; whether the job's schedd is at its MaxJobsRunning cap or overloaded;
    and the job's user's place in the fair-share order, from user_priority. For a job
    that is not idle it says what state the job is in instead. Scheduler-universe
    (DAGMan), local-universe and grid-universe jobs never go to a slot, so for those it
    gives only the reasons that apply: the schedd's policy for starting them on the
    submit host, or the remote system a grid job waits on."""
    log.info("why_idle %d.%d", cluster_id, proc_id)
    found = require_job(cluster_id, proc_id, [])  # the whole ad: a slot's START may read any attribute
    job = found.ad
    out: dict[str, Any] = pool.tag_schedd(
        {"job": f"{cluster_id}.{proc_id}", "status": classads.status_name(job)}, found.schedd
    )
    warnings = list(found.warnings)
    if not found.in_queue or get(job, "JobStatus") != JobStatus.IDLE:
        return {**out, **_not_idle(job, found.in_queue)}

    schedd_ad, schedd = None, None
    try:
        ads = pool.schedd_ads()
        name, schedd_ad = (found.schedd, ads.get(found.schedd)) if pool.settings.schedds else next(iter(ads.items()))
        schedd = classads.schedd_health(name, schedd_ad)
    except ToolError as exc:
        warnings.append(str(exc))
    out["idle_since"] = classads.ts(get(job, "EnteredCurrentStatus"))
    unmatched = diagnose.universe_reasons(job, schedd_ad)
    if unmatched is not None:  # slots and priority do not apply
        out.update({"reasons": unmatched, "schedd_health": schedd})
        if warnings:
            out["warnings"] = warnings
        return out

    slots = pool.query_slots([], "DynamicSlot =!= true")
    if not slots:
        raise ToolError(no_slots())
    matched = diagnose.match(job, slots)

    priority, competing = None, 0
    submitter = submitter_name(job)
    try:
        table = priority_table()
        if submitter:
            # The job is idle, so its user is waiting even before their submitter ad says so
            priority, competing = priority_row(submitter, table), len(table.competing | {submitter})
        if table.warning:
            warnings.append(table.warning)
    except ToolError as exc:
        warnings.append(str(exc))

    reasons = diagnose.reasons(matched, schedd, priority, competing)
    if get(job, "ConcurrencyLimits"):
        reasons.append(
            f"The job uses concurrency limits ({get(job, 'ConcurrencyLimits')}); the negotiator "
            "also holds it back while those limits are used up."
        )
    out.update(
        {
            "reasons": reasons,
            "slot_filter": pool.settings.slot_constraint,
            **matched,
            "schedd_health": schedd,
            "priority": priority,
        }
    )
    if get(job, "LastRejMatchReason"):
        out["negotiator_last_said"] = {
            "reason": get(job, "LastRejMatchReason"),
            "at": classads.ts(get(job, "LastRejMatchTime")),
        }
    if get(job, "NumJobStarts", 0):
        out["times_started_before"] = get(job, "NumJobStarts")
    if warnings:
        out["warnings"] = warnings
    log.info("  -> %d of %d slots fit, %d free now", matched["accept_the_job"], len(slots), matched["free_now"])
    return out
