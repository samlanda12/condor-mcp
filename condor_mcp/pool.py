"""Where the pool is and how to reach it.

Everything pool-specific is read from CONDOR_MCP_* environment variables, with
defaults taken from the local HTCondor config (local schedd, default collector,
the user the server runs as). See README.md for the list.
"""

from __future__ import annotations

import getpass
import logging
from pathlib import Path
from typing import Annotated, Any, Iterator

import classad2 as classad
import htcondor2 as htcondor
from fastmcp.exceptions import ToolError
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

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


def collector() -> htcondor.Collector:
    return htcondor.Collector(settings.collector)


def schedds() -> Iterator[tuple[str, htcondor.Schedd]]:
    """Yield (name, Schedd) for every configured schedd, or the local one."""
    if not settings.schedds:
        yield "local", htcondor.Schedd()
        return
    coll = collector()
    for name in settings.schedds:
        try:
            ad = coll.locate(htcondor.DaemonType.Schedd, name)
        except htcondor.HTCondorException as exc:
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


def multi_schedd() -> bool:
    return len(settings.schedds) > 1


def query_jobs(
    constraint: str,
    projection: list[str],
    limit: int = -1,
) -> list[tuple[str, classad.ClassAd]]:
    """Run one job query against every configured schedd and merge the results.

    Returns (schedd name, ad) pairs. The limit applies per schedd."""
    found: list[tuple[str, classad.ClassAd]] = []
    for name, schedd in schedds():
        try:
            ads = schedd.query(constraint=constraint, projection=projection, limit=limit)
        except htcondor.HTCondorException as exc:  # schedd down, auth, etc.
            raise ToolError(
                f"Could not reach the schedd ({name}): {exc}. Is the schedd up? "
                "Check with `condor_status -schedd`, and CONDOR_MCP_SCHEDDS if set."
            ) from exc
        found.extend((name, ad) for ad in ads)
    return found


def query_slots(projection: list[str], constraint: str | None = None) -> list[classad.ClassAd]:
    """Startd ads from the collector, filtered by the configured slot constraint."""
    expr = settings.slot_constraint
    if constraint:
        expr = f"({expr}) && ({constraint})"
    try:
        return collector().query(htcondor.AdType.Startd, constraint=expr, projection=projection)
    except htcondor.HTCondorException as exc:
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
