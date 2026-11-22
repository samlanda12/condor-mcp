"""The FastMCP instance the tool modules register on, and what they share."""

from __future__ import annotations

from collections import Counter
from typing import Any, NamedTuple

import classad2 as classad
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from condor_mcp import classads, pool
from condor_mcp.classads import get

mcp = FastMCP("condor-mcp")

READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}
# Changes the queue; a client should confirm with the user before calling.
WRITE = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False}
# Changes a job's state. Repeating the call leaves the job as it is, so idempotent.
ACT = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True}
# Takes a job out of the queue for good; a client should warn the user before calling.
DESTRUCTIVE = {**ACT, "destructiveHint": True}

OwnerField = Field(description="Whose jobs: 'me' for the user this server runs as, 'all' for everyone, or a username.")
ClusterField = Field(ge=1, description="Cluster number, the part before the dot in 123.0")


def job_projection(base: list[str]) -> list[str]:
    return base + [a for a in pool.settings.job_attrs if a not in base]


def require_job(cluster_id: int, proc_id: int, projection: list[str]) -> pool.FoundJob:
    """pool.find_job, with a job in neither the queue nor the history as a ToolError."""
    found = pool.find_job(cluster_id, proc_id, projection)
    if found is None:
        raise ToolError(
            f"Job {cluster_id}.{proc_id} is in neither the queue nor the schedd history. The ID "
            "may be wrong, or the job is old enough that the schedd has dropped it from its history."
        )
    return found


def no_slots() -> str:
    return (
        "The collector returned no slots. Either no execute machines are "
        "reporting or the slot filter (CONDOR_MCP_SLOT_CONSTRAINT = "
        f"{pool.settings.slot_constraint!r}) excludes everything."
    )


def submitter_name(job: classad.ClassAd) -> str | None:
    """The name the negotiator knows the job's user by: user@domain, or group.user@domain
    for a job submitted under an accounting group."""
    user, group = get(job, "User"), get(job, "AccountingGroup")
    if user and group:
        return f"{group}@{user.rsplit('@', 1)[-1]}"
    return user


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
