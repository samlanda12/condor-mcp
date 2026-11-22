"""Tools that hold, release and remove jobs."""

from __future__ import annotations

import getpass
from typing import Annotated, Any

import htcondor2 as htcondor
from fastmcp.exceptions import ToolError
from htcondor2 import JobStatus
from pydantic import Field

from condor_mcp import classads, pool, worker
from condor_mcp.app import ACT, DESTRUCTIVE, ClusterField, mcp
from condor_mcp.classads import get
from condor_mcp.pool import log
from condor_mcp.worker import NoAnswer

ProcField = Field(ge=0, description="Process number within the cluster. Omit to act on every job in the cluster.")
RemoveProcField = Field(
    ge=0,
    description="Process number within the cluster. Give this, or all_procs=True, never both.",
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
            "action": verb,
            "reason": reason,
            "changed": 0,
            "jobs": [],
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
    others = sorted({get(ad, "Owner") or "<no owner>" for _, ad in found} - {pool.settings.user})
    if others:
        raise ToolError(
            f"Refusing to {verb} {label}: it belongs to {', '.join(others)}, not {pool.settings.user}. "
            "This server only acts on jobs owned by the user it runs as (CONDOR_MCP_USER)."
        )
    (name,) = names
    schedd = by_name[name]
    before = {classads.job_id(ad): ad for _, ad in found}
    mine, _ = pool.owner_constraint("me")  # so the schedd enforces the owner check too
    try:
        # htcondor2 drops a plain-string reason on the floor; only the (text, code) tuple reaches
        # the schedd. A None code keeps HoldReasonCode at 1, the same as condor_hold.
        result = pool.call(worker.schedd_act, schedd, action, f"{constraint} && {mine}", (reason, None))
    except NoAnswer as exc:
        raise ToolError(
            f"The schedd ({name}) did not answer the request to {verb} {label}: {exc}. It may or may "
            "not have taken effect; check with job_details before trying again."
        ) from exc
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
    after = {classads.job_id(ad): ad for ad in pool.query_schedd(name, schedd, constraint, _ACT_PROJECTION)}
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
