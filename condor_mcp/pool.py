"""Where the pool is and how to reach it.

Everything pool-specific is read from CONDOR_MCP_* environment variables, with
defaults taken from the local HTCondor config (local schedd, default collector,
the user the server runs as). See README.md for the list.
"""

from __future__ import annotations

import getpass
import logging
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import classad2 as classad
import htcondor2 as htcondor
from fastmcp.exceptions import ToolError
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from condor_mcp import worker
from condor_mcp.classads import get
from condor_mcp.worker import NoAnswer

log = logging.getLogger("condor_mcp")

# Comma-separated in the environment; NoDecode stops pydantic-settings from
# trying to parse the value as JSON first.
CsvList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CONDOR_MCP_", env_ignore_empty=True, frozen=True)

    # Collector host[:port]; empty means COLLECTOR_HOST from the HTCondor config.
    collector: str | None = None
    # Schedd names to query, looked up through the collector. Empty means the
    # local schedd. Several names are queried at once and merged.
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
    # Seconds to wait for a schedd, the collector or the negotiator before giving up on a call. Capped.
    timeout: float = Field(default=60, gt=0, le=3600)

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


def call[T](fn: Callable[..., T], *args: Any) -> T:
    """Run one of the calls in condor_mcp.worker with the configured deadline."""
    try:
        return worker.run(settings.timeout, fn, *args)
    except NoAnswer as exc:
        if not exc.timed_out:
            raise
        raise NoAnswer(f"{exc}; CONDOR_MCP_TIMEOUT sets the limit", timed_out=True) from None


def schedd_names() -> list[str]:
    """The configured schedds, or "local" for the local one."""
    return settings.schedds or ["local"]


def locate(name: str) -> htcondor.Schedd:
    """The Schedd for one of schedd_names(), looked up through the collector."""
    if not settings.schedds:
        return htcondor.Schedd()
    try:
        ad = call(worker.collector_locate, settings.collector, htcondor.DaemonType.Schedd, name)
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not reach the collector to find schedd {name!r}: {exc}. Check CONDOR_MCP_COLLECTOR."
        ) from exc
    if ad is None:  # locate() returns None rather than raising for an unknown name
        raise ToolError(
            f"The collector knows no schedd named {name!r}. Check CONDOR_MCP_SCHEDDS against `condor_status -schedd`."
        )
    return htcondor.Schedd(ad)


def schedds() -> Iterator[tuple[str, htcondor.Schedd]]:
    """Yield (name, Schedd) for every configured schedd, or the local one. One that
    cannot be found fails the whole call; the write tools use this."""
    for name in schedd_names():
        yield name, locate(name)


def tag_schedd(row: dict[str, Any], name: str) -> dict[str, Any]:
    """Add the schedd name to a result row when more than one schedd is configured,
    so a job ID can be traced to its queue. Returns the row."""
    if len(settings.schedds) > 1:
        row["schedd"] = name
    return row


Ads = list[tuple[str, classad.ClassAd]]


def ask_schedds(
    ask: Callable[[str], list[classad.ClassAd]], failed: dict[str, str] | None = None
) -> tuple[Ads, dict[str, str]]:
    """Run ask(name) for every configured schedd at once and merge the ads into (schedd
    name, ad) pairs, so a schedd that does not answer costs one timeout, not one each.

    A schedd whose ask raises ToolError is left out. Returns the pairs and failed, updated
    with why each such schedd failed; schedds already in failed are not asked again.
    Raises ToolError only when every schedd has failed."""
    failed = dict(failed or {})
    names = [name for name in schedd_names() if name not in failed]
    with ThreadPoolExecutor(max_workers=len(names) or 1) as threads:
        futures = [(name, threads.submit(ask, name)) for name in names]
    found: Ads = []
    for name, future in futures:
        try:
            found += [(name, ad) for ad in future.result()]
        except ToolError as exc:
            failed[name] = str(exc)
    if len(failed) == len(schedd_names()):
        raise ToolError(" ".join(failed.values()))
    return found, failed


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
    failed: dict[str, str] | None = None,
) -> tuple[Ads, dict[str, str]]:
    """Run one job query against every configured schedd and merge the results.

    Returns (schedd name, ad) pairs and, for each schedd that could not be read, why; see
    ask_schedds. The limit applies per schedd."""
    return ask_schedds(lambda name: query_schedd(name, locate(name), constraint, projection, limit), failed)


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
) -> tuple[Ads, dict[str, str]]:
    """Read the job history of every configured schedd: jobs that completed or were
    removed and have left the queue.

    Returns (schedd name, ad) pairs, most recently finished first, and the schedds that
    could not be read, as query_jobs does. The limit applies per schedd, so the merged
    list can be longer; callers trim it."""
    found, failed = ask_schedds(lambda name: query_schedd_history(name, locate(name), constraint, projection, limit))
    # Each schedd answers newest first; the sort interleaves several schedds the same way
    found.sort(key=lambda pair: get(pair[1], "EnteredCurrentStatus", 0), reverse=True)
    return found, failed


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
    not at all with a single schedd.

    A schedd that cannot be read does not fail the lookup of a queued job: it comes back
    with a warning that the ID could not be checked there. A job in no queue that answered
    fails instead, since the job may be on the schedd that did not."""
    constraint = f"ClusterId == {cluster_id} && ProcId == {proc_id}"
    projection = [*projection, "QDate"] if projection else []  # an empty projection is the whole ad
    in_queues, failed = query_jobs(constraint, projection, 1)
    found = [FoundJob(name, ad, True, []) for name, ad in in_queues]
    if not found:
        if failed:
            raise ToolError(" ".join(failed.values()))
        finished, failed = query_history(constraint, projection, 1)
        if failed:
            raise ToolError(" ".join(failed.values()))
        found = [FoundJob(name, ad, False, []) for name, ad in finished]
    elif len(found) == 1 and len(settings.schedds) > 1:
        # The queued job's own schedd never reuses its ID, and reading its history could
        # find the job a second time if it leaves the queue between the two reads
        queued = found[0]
        since = f"EnteredCurrentStatus < {get(queued.ad, 'QDate', 0)}"

        def other_history(name: str) -> list[classad.ClassAd]:
            if name == queued.schedd:
                return []
            return query_schedd_history(name, locate(name), constraint, projection, 1, since)

        finished, failed = ask_schedds(other_history, failed)
        found += [FoundJob(name, ad, False, []) for name, ad in finished]
        queued.warnings.extend(
            f"{why} So schedd {name} could not be checked for another job "
            f"{cluster_id}.{proc_id}; this is the one queued on {queued.schedd}."
            for name, why in failed.items()
        )
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


SCHEDD_PROJECTION = [
    "Name", "RecentDaemonCoreDutyCycle", "MaxJobsRunning", "TotalRunningJobs", "TotalIdleJobs", "TotalHeldJobs",
]


def schedd_ads() -> dict[str, classad.ClassAd | None]:
    """Each configured schedd's ad from the collector, or the local schedd's, by name.
    A configured schedd the collector has no ad for maps to None."""
    try:
        if not settings.schedds:
            ad = call(worker.collector_locate, settings.collector, htcondor.DaemonType.Schedd, None)
            return {get(ad, "Name", "local"): ad} if ad is not None else {"local": None}
        names = " || ".join(f"Name == {classad.quote(name)}" for name in settings.schedds)
        ads = call(worker.collector_query, settings.collector, htcondor.AdType.Schedd, names, SCHEDD_PROJECTION)
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not read the schedd ads from the collector "
            f"({settings.collector or 'from HTCondor config'}): {exc}."
        ) from exc
    found = {get(ad, "Name"): ad for ad in ads}
    return {name: found.get(name) for name in settings.schedds}


def query_submitters() -> list[classad.ClassAd]:
    """Submitter ads from the collector: one per user and schedd with jobs in that queue
    now, with the user's idle, running and held counts there."""
    try:
        return call(
            worker.collector_query, settings.collector, htcondor.AdType.Submitter, "True",
            ["Name", "IdleJobs", "RunningJobs", "HeldJobs"],
        )
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not reach the collector ({settings.collector or 'from HTCondor config'}): "
            f"{exc}. Is the pool up? Check with `condor_status`, and CONDOR_MCP_COLLECTOR if set."
        ) from exc


def locate_negotiator() -> classad.ClassAd | None:
    """The configured collector's negotiator, or None for the local config's."""
    if not settings.collector:
        return None
    try:
        return call(worker.collector_locate, settings.collector, htcondor.DaemonType.Negotiator, None)
    except CONDOR_ERRORS as exc:
        raise ToolError(
            f"Could not find the negotiator through the collector ({settings.collector}): {exc}. "
            "Check CONDOR_MCP_COLLECTOR."
        ) from exc


# The negotiator updates priorities once per negotiation cycle and answers no queries while
# it negotiates, which on a busy pool can take minutes, so its answer is kept this long.
PRIORITY_MAX_AGE = 300
_priorities: tuple[float, list[classad.ClassAd]] | None = None
_priorities_lock = threading.Lock()


def accountant_ads() -> tuple[list[classad.ClassAd], float]:
    """The negotiator's accounting ads and the time.time() they were read, at most
    PRIORITY_MAX_AGE seconds ago. Callers wait on one read rather than each making one."""
    global _priorities
    with _priorities_lock:
        if _priorities is None or time.time() - _priorities[0] > PRIORITY_MAX_AGE:
            try:
                ads = call(worker.negotiator_priorities, locate_negotiator())
            except CONDOR_ERRORS as exc:
                raise ToolError(
                    f"Could not get user priorities from the negotiator: {exc}. Is it up? "
                    "Check with `condor_userprio`, and CONDOR_MCP_COLLECTOR if set."
                ) from exc
            _priorities = (time.time(), ads)
        return _priorities[1], _priorities[0]


def owner_constraint(owner: str) -> tuple[str, str | None]:
    """Translate the owner argument tools accept into (constraint, owner name).

    'me' is the configured user, 'all' is everyone, anything else is a username."""
    if owner == "all":
        return "True", None
    name = settings.user if owner in ("me", "") else owner
    return f"Owner == {classad.quote(name)}", name
