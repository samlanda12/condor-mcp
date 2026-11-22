"""Tools that read the pool: its capacity, its schedds, and fair-share priority."""

from __future__ import annotations

from collections import Counter
from typing import Annotated, Any, NamedTuple

import classad2 as classad
from fastmcp.exceptions import ToolError
from pydantic import Field

from condor_mcp import classads, pool
from condor_mcp.app import READ_ONLY, OwnerField, mcp, no_slots
from condor_mcp.classads import get
from condor_mcp.pool import log


@mcp.tool(annotations=READ_ONLY, tags={"pool", "read-only"})
def pool_status(
    detail: Annotated[bool, Field(description="Also return one row per slot.")] = False,
) -> dict[str, Any]:
    """Report the pool's capacity from the collector: machines, slots by state,
    CPUs, memory and GPUs in total and currently available, GPUs by model where
    the pool reports one, and the largest block of free CPUs on any one machine.
    Also the health of each schedd the server uses: its recent duty cycle with a load
    name (ok below 0.50, busy, strained from 0.80, overloaded from 0.95, when it is slow
    to start jobs and answer queries), its running, idle and held jobs, and its
    MaxJobsRunning cap, past which none of its jobs start.
    Use this for "is the cluster busy" or "are there free GPUs"; for why one job is
    still idle, use why_idle. Slot counts include dynamic slots, so a machine with a
    partitionable slot shows one unclaimed slot holding its spare resources plus
    one claimed slot per running job. Which slots count is set by the server's
    slot filter."""
    log.info("pool_status detail=%s", detail)
    projection = classads.SLOT_PROJECTION + [a for a in pool.settings.slot_attrs if a not in classads.SLOT_PROJECTION]
    ads = pool.query_slots(projection)
    if not ads:
        raise ToolError(no_slots())

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
            model = classads.gpu_model(ad) or "unknown model"
            entry = gpu_models.setdefault(model, {"total": 0, "available": 0})
            entry["total"] += total_g
            entry["available"] += free_g
        if largest is None or free_c > largest["free_cpus"]:
            largest = {"machine": get(ad, "Machine"), "free_cpus": free_c, "machine_cpus": total_c}

    out = {
        "collector": pool.settings.collector or "from HTCondor config",
        "slot_filter": pool.settings.slot_constraint,
        "machines": len(machines),
        "slots": {"total": len(ads), "by_state": dict(by_state)},
        "cpus": cpus,
        "memory_mb": memory,
        "gpus": {**gpus, "by_model": gpu_models},
        "largest_free_cpu_block": largest,
    }
    try:
        out["schedds"] = [classads.schedd_health(name, ad) for name, ad in pool.schedd_ads().items()]
    except ToolError as exc:  # the slots are still worth returning
        out["warnings"] = [str(exc)]
    if detail:
        out["slot_list"] = [classads.summarize_slot(ad, pool.settings.slot_attrs) for ad in ads]
    log.info("  -> %d slots, %d/%d cpus free", len(ads), cpus["available"], cpus["total"])
    return out


def submitter_name(job: classad.ClassAd) -> str | None:
    """The name the negotiator knows the job's user by: user@domain, or group.user@domain
    for a job submitted under an accounting group."""
    user, group = get(job, "User"), get(job, "AccountingGroup")
    if user and group:
        return f"{group}@{user.rsplit('@', 1)[-1]}"
    return user


def _group_submitters(constraint: str) -> tuple[set[str], list[str]]:
    """The accounting-group submitter names, such as group_physics.alice@domain, that the
    owner's queued jobs run under, and why any schedd could not be read. The name alone
    cannot tell a group prefix from a dotted username (john.alice), and without GROUP_NAMES
    the negotiator does not record the group, so the jobs are the only reliable source."""
    try:
        found, failed = pool.query_jobs(f"({constraint}) && AccountingGroup =!= undefined", ["User", "AccountingGroup"])
    except ToolError as exc:
        return set(), [f"Could not read the queues for the user's accounting groups: {exc}"]
    return {name for _, ad in found if (name := submitter_name(ad))}, list(failed.values())


class _PriorityTable(NamedTuple):
    records: dict[str, classad.ClassAd]  # accountant ad by submitter name
    jobs: dict[str, Counter[str]]  # idle, running and held jobs by submitter name
    competing: set[str]  # submitters with idle jobs, the ones waiting for slots
    read_at: float
    warning: str | None  # the negotiator did not answer, so the records are older


def priority_table() -> _PriorityTable:
    """The negotiator's records for each submitter, joined with the submitter ads."""
    ads, read_at, warning = pool.accountant_ads()
    records = {get(ad, "Name"): ad for ad in ads if not get(ad, "IsAccountingGroup", False)}
    jobs: dict[str, Counter[str]] = {}
    for ad in pool.query_submitters():
        counts = jobs.setdefault(get(ad, "Name"), Counter())
        for key, attr in (("idle", "IdleJobs"), ("running", "RunningJobs"), ("held", "HeldJobs")):
            counts[key] += int(get(ad, attr, 0) or 0)
    competing = {user for user, counts in jobs.items() if counts["idle"]}
    return _PriorityTable(records, jobs, competing, read_at, warning)


def priority_row(user: str, table: _PriorityTable) -> dict[str, Any]:
    """One user_priority row, with the user's place in line: 1 plus the users with idle
    jobs and a better priority. A user with no record yet has the best priority."""

    def priority(name: str) -> float:
        return float(get(table.records[name], "Priority", 0)) if name in table.records else float("-inf")

    row: dict[str, Any] = classads.priority_row(table.records[user]) if user in table.records else {"user": user}
    if user not in table.records:
        row["note"] = "The negotiator has no usage record yet, so this user starts at the best priority."
    mine = priority(user)
    row["rank"] = 1 + sum(1 for u in table.competing if u != user and priority(u) < mine)
    row["jobs"] = {key: table.jobs.get(user, Counter())[key] for key in ("idle", "running", "held")}
    return row


@mcp.tool(annotations=READ_ONLY, tags={"pool", "read-only"})
def user_priority(owner: Annotated[str, OwnerField] = "me") -> dict[str, Any]:
    """Report fair-share priority from the negotiator. When more jobs wait than slots
    are free, the negotiator offers slots to users in order of effective priority,
    lowest (best) first. A user's priority worsens with their recent usage, which decays
    with a half-life (a day by default), times a per-user factor that admins set.

    Each row has the effective priority, the real priority and factor it is the product
    of, CPUs in use now and total usage hours (both weighted, by CPUs unless the pool
    weights slots otherwise), the user's idle, running and held jobs, and their rank
    among the users with idle jobs now (1 is offered slots first; for a user with no
    idle jobs, where their next one would be). A user with no usage record yet counts as
    the best priority. A user who submits under accounting groups has one row per group
    they have jobs queued in. 'all' lists the users with jobs in a queue now. Job counts
    come from the schedds' submitter ads, which can trail the queue by a few minutes.
    The negotiator's answer is reused for up to five minutes; as_of says when it was
    read. When the negotiator does not answer, the last answer is returned with a
    warning."""
    log.info("user_priority owner=%s", owner)
    constraint, name = pool.owner_constraint(owner)
    table = priority_table()
    warnings = [table.warning] if table.warning else []
    if name is None:
        users = [user for user, counts in table.jobs.items() if sum(counts.values())]
    else:
        groups, failed = _group_submitters(constraint)
        warnings += failed
        users = [user for user in {*table.records, *table.jobs} if user.rsplit("@", 1)[0] == name or user in groups]
    rows = [priority_row(user, table) for user in users]
    rows.sort(key=lambda row: row.get("effective_priority", float("-inf")))

    out: dict[str, Any] = {
        "as_of": classads.ts(table.read_at),
        "users_competing": len(table.competing),
        "users": rows,
    }
    if not rows:
        out["note"] = (
            "No user has jobs in a queue."
            if name is None
            else f"The negotiator has no record of {name} and they have no jobs in a queue. "
            "A user's first jobs start at the best priority."
        )
    if warnings:
        out["warnings"] = warnings
    log.info("  -> %d rows, %d users competing", len(rows), len(table.competing))
    return out
