"""Where the pool is and how to reach it.

Everything pool-specific is read from CONDOR_MCP_* environment variables, with
defaults taken from the local HTCondor config (local schedd, default collector,
the user the server runs as). See README.md for the list.
"""

from __future__ import annotations

import getpass
import logging
from pathlib import Path
from typing import Annotated, Any, Callable, Iterator, NamedTuple, TypeVar

import classad2 as classad
import htcondor2 as htcondor
from fastmcp.exceptions import ToolError
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from condor_mcp import worker
from condor_mcp.classads import get
from condor_mcp.worker import NoAnswer

T = TypeVar("T")

log = logging.getLogger("condor_mcp")

# Comma-separated in the environment; NoDecode stops pydantic-settings from
# trying to parse the value as JSON first.
CsvList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CONDOR_MCP_", env_ignore_empty=True, frozen=True)

    # Collector host[:port]; empty means COLLECTOR_HOST from the HTCondor config.
    collector: str | None = None
    # Schedd names to query, looked up through the collector. Empty means the
    # local schedd. Several names are queried in turn and merged.
    schedds: CsvList = []
    # Whose jobs "my jobs" means.
    user: str = Field(default_factory=getpass.getuser)
    # ClassAd expression every slot must satisfy to count in pool_status.
    slot_constraint: str = "True"
    # Site-specific attributes to add to job / slot projections and output.
    job_attrs: CsvList = []
    slot_attrs: CsvList = []
    # Append one line per tool call to this file; empty = off.
    call_log: str | None = None
    # Where drafted jobs run from (their initialdir) and where their log,
    # stdout and stderr land.
    workdir: Path = Field(default_factory=lambda: Path.home() / "condor-mcp-jobs")
    # Seconds to wait for a schedd or the collector before giving up on a call.
    timeout: float = Field(default=60, gt=0)

    @field_validator("schedds", "job_attrs", "slot_attrs", mode="before")
    @classmethod
    def _split_csv(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value

    @field_validator("workdir", mode="before")
    @classmethod
    def _absolute(cls, value: Any) -> Any:
        return Path(value).expanduser().absolute() if isinstance(value, (str, Path)) else value


settings = Settings()

if settings.call_log:
    _handler = logging.FileHandler(settings.call_log)
    _handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
    log.addHandler(_handler)
    log.setLevel(logging.INFO)

# condor_submit refuses a job whose initialdir is missing, and drafts set it to the
# work directory, so make it now rather than on the first submit. A failure is
# left to the tools to report: draft_submit warns and submit_job refuses.
try:
    settings.workdir.mkdir(parents=True, exist_ok=True)
except OSError as _exc:
    log.warning("could not create the work directory %s: %s", settings.workdir, _exc)


# What a call to a daemon raises when it fails or gets no answer in time
CONDOR_ERRORS = (htcondor.HTCondorException, NoAnswer)


def call(fn: Callable[..., T], *args: Any) -> T:
    """Run one of the calls in condor_mcp.worker with the configured deadline."""
    try:
        return worker.run(settings.timeout, fn, *args)
    except NoAnswer as exc:
        raise NoAnswer(f"{exc}; CONDOR_MCP_TIMEOUT sets the limit") from None


def schedds() -> Iterator[tuple[str, htcondor.Schedd]]:
    """Yield (name, Schedd) for every configured schedd, or the local one."""
    if not settings.schedds:
        yield "local", htcondor.Schedd()
        return
    for name in settings.schedds:
        try:
            ad = call(worker.collector_locate, settings.collector, htcondor.DaemonType.Schedd, name)
        except CONDOR_ERRORS as exc:
            raise ToolError(
                f"Could not reach the collector to find schedd {name!r}: {exc}. "
                "Check CONDOR_MCP_COLLECTOR."
            ) from exc
        if ad is None:  # locate() returns None rather than raising for an unknown name
            raise ToolError(
                f"The collector knows no schedd named {name!r}. Check CONDOR_MCP_SCHEDDS "
                "against `condor_status -schedd`."
            )
        yield name, htcondor.Schedd(ad)


def tag_schedd(row: dict[str, Any], name: str) -> dict[str, Any]:
    """Add the schedd name to a result row when more than one schedd is configured,
    so a job ID can be traced to its queue. Returns the row."""
    if len(settings.schedds) > 1:
        row["schedd"] = name
    return row


def query_schedd(
    name: str,
    schedd: htcondor.Schedd,
    constraint: str,
    projection: list[str],
    limit: int = -1,
) -> list[classad.ClassAd]:
    """Run one job query against one schedd, turning a failure into a ToolError."""
    try:
        return call(worker.schedd_query, schedd, constraint, projection, limit)
    except CONDOR_ERRORS as exc:  # schedd down or not answering, auth, etc.
        raise ToolError(
            f"Could not reach the schedd ({name}): {exc}. Is the schedd up? "
            "Check with `condor_status -schedd`, and CONDOR_MCP_SCHEDDS if set."
        ) from exc


def query_jobs(
    constraint: str,
    projection: list[str],
    limit: int = -1,
) -> list[tuple[str, classad.ClassAd]]:
    """Run one job query against every configured schedd and merge the results.

    Returns (schedd name, ad) pairs. The limit applies per schedd."""
    return [
        (name, ad)
        for name, schedd in schedds()
        for ad in query_schedd(name, schedd, constraint, projection, limit)
    ]


def query_schedd_history(
    name: str,
    schedd: htcondor.Schedd,
    constraint: str,
    projection: list[str],
    limit: int,
    since: str | None = None,
) -> list[classad.ClassAd]:
    """Read one schedd's job history, turning a failure into a ToolError. The history is
    read newest first and the read stops at the first job for which since is true."""
    try:
        return call(worker.schedd_history, schedd, constraint, projection, limit, since)
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not read the job history of the schedd ({name}): {exc}. Is the schedd "
            "up? Check with `condor_status -schedd`, and CONDOR_MCP_SCHEDDS if set."
        ) from exc


def query_history(
    constraint: str,
    projection: list[str],
    limit: int,
) -> list[tuple[str, classad.ClassAd]]:
    """Read the job history of every configured schedd: jobs that completed or were
    removed and have left the queue.

    Returns (schedd name, ad) pairs, most recently finished first. The limit applies
    per schedd, so the merged list can be longer; callers trim it."""
    found = [
        (name, ad)
        for name, schedd in schedds()
        for ad in query_schedd_history(name, schedd, constraint, projection, limit)
    ]
    # Each schedd answers newest first; the sort interleaves several schedds the same way
    found.sort(key=lambda pair: get(pair[1], "EnteredCurrentStatus", 0), reverse=True)
    return found


class FoundJob(NamedTuple):
    schedd: str
    ad: classad.ClassAd
    in_queue: bool
    # Schedds that could not be checked for another job with the same ID, and why
    warnings: list[str]


def find_job(cluster_id: int, proc_id: int, projection: list[str]) -> FoundJob | None:
    """One job's ad from the queue or, once it has left the queue, from the history.

    Returns None if neither has it. Cluster IDs are only unique within one schedd, so an
    ID found on more than one is refused rather than guessed. The exception is a queued
    job and a history hit on another schedd that left the queue before the queued job was
    submitted: that is an older job that reused the ID, so the queued job wins. Once a
    queue has the job, only the other schedds' histories are read, only back that far, and
    not at all with a single schedd. One of those that cannot be read does not fail the
    lookup: the queued job comes back with a warning that the ID could not be checked
    there."""
    constraint = f"ClusterId == {cluster_id} && ProcId == {proc_id}"
    projection = [*projection, "QDate"]
    found = [FoundJob(name, ad, True, []) for name, ad in query_jobs(constraint, projection, 1)]
    if not found:
        found = [FoundJob(name, ad, False, []) for name, ad in query_history(constraint, projection, 1)]
    elif len(found) == 1 and len(settings.schedds) > 1:
        # The queued job's own schedd never reuses its ID, and reading its history could
        # find the job a second time if it leaves the queue between the two reads
        queued = found[0]
        since = f"EnteredCurrentStatus < {get(queued.ad, 'QDate', 0)}"
        for name, schedd in schedds():
            if name == queued.schedd:
                continue
            try:
                ads = query_schedd_history(name, schedd, constraint, projection, 1, since)
            except ToolError as exc:
                queued.warnings.append(
                    f"{exc} So schedd {name} could not be checked for a newer job "
                    f"{cluster_id}.{proc_id}; this is the one queued on {queued.schedd}."
                )
                continue
            found += [FoundJob(name, ad, False, []) for ad in ads]
    if len(found) > 1:
        raise ToolError(
            f"Job {cluster_id}.{proc_id} exists on more than one schedd "
            f"({', '.join(job.schedd for job in found)}), so the ID is ambiguous. "
            "Set CONDOR_MCP_SCHEDDS to the one that holds the job."
        )
    return found[0] if found else None


def query_slots(projection: list[str], constraint: str | None = None) -> list[classad.ClassAd]:
    """Startd ads from the collector, filtered by the configured slot constraint."""
    expr = settings.slot_constraint
    if constraint:
        expr = f"({expr}) && ({constraint})"
    try:
        return call(worker.collector_query, settings.collector, htcondor.AdType.Startd, expr, projection)
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not reach the collector ({settings.collector or 'from HTCondor config'}): "
            f"{exc}. Is the pool up? Check with `condor_status`, and CONDOR_MCP_COLLECTOR if set."
        ) from exc


def owner_constraint(owner: str) -> tuple[str, str | None]:
    """Translate the owner argument tools accept into (constraint, owner name).

    'me' is the configured user, 'all' is everyone, anything else is a username."""
    if owner == "all":
        return "True", None
    name = settings.user if owner in ("me", "") else owner
    return f"Owner == {classad.quote(name)}", name
