"""The FastMCP instance the tool modules register on, and what they share."""

from __future__ import annotations

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from condor_mcp import pool
from condor_mcp.pool import settings

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
    return base + [a for a in settings.job_attrs if a not in base]


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
        f"{settings.slot_constraint!r}) excludes everything."
    )
