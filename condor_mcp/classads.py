"""Translate ClassAds into the plain dicts the model sees.

The hold-code table lives here too; status names come from htcondor2's JobStatus
enum. Both are translated to words, but the numeric code is always kept in the
output so nothing is lost.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import classad2 as classad
from htcondor2 import JobStatus

# JobStatus code -> the lowercase name the model sees ("idle", "held", ...)
JOB_STATUS = {status.value: status.name.lower() for status in JobStatus}
STATUS_CODE = {name: code for code, name in JOB_STATUS.items()}

# HoldReasonCode -> plain language. Matches the table in the HTCondor manual
HOLD_CODES = {
    1: "user requested hold (condor_hold)",
    3: "the job's own periodic_hold / on_exit_hold expression became true",
    4: "the job's credentials are invalid",
    5: "a job policy expression evaluated to undefined",
    6: "failed to start the program (bad executable or permissions)",
    7: "the job's stdout file could not be opened",
    8: "the job's stdin file could not be opened",
    9: "the job's stdout stream could not be opened",
    10: "the job's stdin stream could not be opened",
    11: "internal HTCondor protocol error while transferring files",
    12: "output file transfer failed",
    13: "input file transfer failed (a file listed in transfer_input_files is missing or unreadable)",
    14: "the submit directory (initialdir) is missing or unreadable",
    15: "submitted on hold (hold = true in the submit file)",
    16: "input files are being spooled",
    17: "standard universe job is not compatible with the available condor_shadow",
    18: "internal HTCondor protocol error while transferring files",
    19: "the site's prepare-job hook failed",
    20: "the job missed its deferred execution time",
    21: "held by the execute machine",
    22: "unable to initialize the job event log",
    23: "failed to access the user account",
    24: "no compatible condor_shadow",
    25: "invalid cron settings",
    26: "held by pool-wide system policy",
    27: "the pool's system periodic policy evaluated to undefined",
    32: "input files exceed the pool's maximum transfer size",
    33: "output files exceed the pool's maximum transfer size",
    34: "job used more memory or disk than it requested",
    35: "container image is invalid",
    36: "job failed when sent the checkpoint signal it requested",
    37: "user error in the EC2 universe",
    38: "internal error in the EC2 universe",
    39: "administrator error in the EC2 universe",
    40: "connection problem in the EC2 universe",
    41: "server error in the EC2 universe",
    42: "instance potentially lost in the EC2 universe",
    43: "DAG pre-script failed",
    44: "DAG post-script failed",
    45: "test of the singularity/apptainer runtime failed before the job launched",
    46: "job exceeded its allowed runtime",
    47: "job exceeded its allowed execution time",
    48: "the site's shadow prepare-job hook asked for the job to be held",
}

# Attributes we ask the schedd for when describing one job in full
JOB_PROJECTION = [
    "ClusterId", "ProcId", "Owner", "JobStatus", "EnteredCurrentStatus",
    "Cmd", "Args", "Arguments", "Iwd",
    "QDate", "JobStartDate", "CompletionDate", "RemoteHost",
    "RequestCpus", "RequestMemory", "RequestDisk", "TransferInputSizeMB",
    "MemoryUsage", "RemoteWallClockTime", "NumJobStarts",
    "HoldReason", "HoldReasonCode", "HoldReasonSubCode",
    "ExitCode", "ExitBySignal", "ExitSignal",
    "Out", "Err", "UserLog", "TransferInput",
]

# Enough for one line per job in a listing
JOB_LIST_PROJECTION = [
    "ClusterId", "ProcId", "Owner", "JobStatus", "EnteredCurrentStatus",
    "Cmd", "Args", "Arguments", "QDate", "RemoteHost",
    "HoldReasonCode", "HoldReason",
]

HELD_PROJECTION = [
    "ClusterId", "ProcId", "Owner", "Cmd", "Args", "Arguments",
    "EnteredCurrentStatus", "NumHolds",
    "HoldReason", "HoldReasonCode", "HoldReasonSubCode", "TransferInput",
]

# Slot attributes from the collector
SLOT_PROJECTION = [
    "Name", "Machine", "State", "Activity", "SlotType",
    "PartitionableSlot", "DynamicSlot",
    "Cpus", "Memory", "Disk", "GPUs",
    "TotalSlotCpus", "TotalSlotMemory", "TotalSlotGPUs",
    "CUDADeviceName", "RemoteOwner", "LoadAvg",
    "EnteredCurrentActivity",
    "TotalSlotDisk", "Arch", "OpSys",
]


def ts(value: Any) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()


def get(ad: classad.ClassAd, attr: str, default: Any = None) -> Any:
    """ad.get() with expressions evaluated; Undefined/Error count as missing."""
    val = ad.get(attr)
    if isinstance(val, classad.ExprTree):
        val = ad.eval(attr)
    if val is None or isinstance(val, classad.Value):
        return default
    return val


def command_line(ad: classad.ClassAd) -> str:
    args = get(ad, "Arguments") or get(ad, "Args")
    return " ".join(s for s in (get(ad, "Cmd"), args) if s)


def job_id(ad: classad.ClassAd) -> str:
    return f"{get(ad, 'ClusterId')}.{get(ad, 'ProcId')}"


def split_files(value: Any) -> list[str]:
    if not value:
        return []
    return [f.strip() for f in str(value).split(",") if f.strip()]


def requested(ad: classad.ClassAd) -> dict[str, Any]:
    """What a job asked for, in HTCondor's units."""
    return {
        "cpus": get(ad, "RequestCpus"),
        "memory_mb": get(ad, "RequestMemory"),
        "disk_kb": get(ad, "RequestDisk"),
        "gpus": get(ad, "RequestGPUs", 0),
    }


def job_files(ad: classad.ClassAd) -> dict[str, Any]:
    return {
        "stdout": get(ad, "Out"),
        "stderr": get(ad, "Err"),
        "log": get(ad, "UserLog"),
        "transfer_input": split_files(get(ad, "TransferInput")),
    }


SLOT_RESOURCES = ("Cpus", "Memory", "Disk", "GPUs")


def slot_totals(ad: classad.ClassAd) -> dict[str, int]:
    """A slot's full size by resource, whether it is partitionable (TotalSlotX)
    or static (X). Site attribute names differ, so this is the one place to adapt."""
    return {res: int(get(ad, f"TotalSlot{res}", get(ad, res, 0)) or 0) for res in SLOT_RESOURCES}


def site_attrs(ad: classad.ClassAd, attrs: Iterable[str]) -> dict[str, Any]:
    return {attr: get(ad, attr) for attr in attrs}


def hold_info(ad: classad.ClassAd) -> dict[str, Any]:
    code = get(ad, "HoldReasonCode")
    return {
        "reason": get(ad, "HoldReason"),
        "code": code,
        "subcode": get(ad, "HoldReasonSubCode"),
        "meaning": HOLD_CODES.get(code, "see HoldReasonCode in the HTCondor manual"),
    }


def summarize_job(ad: classad.ClassAd, extra_attrs: Iterable[str] = ()) -> dict[str, Any]:
    status_code = get(ad, "JobStatus")
    out = {
        "job_id": job_id(ad),
        "owner": get(ad, "Owner"),
        "status": JOB_STATUS.get(status_code, f"unknown({status_code})"),
        "status_since": ts(get(ad, "EnteredCurrentStatus")),
        "command": command_line(ad),
        "submit_dir": get(ad, "Iwd"),
        "submitted_at": ts(get(ad, "QDate")),
        "started_at": ts(get(ad, "JobStartDate")),
        "running_on": get(ad, "RemoteHost"),
        "requested": requested(ad),
        "usage": {
            "memory_mb": get(ad, "MemoryUsage"),
            "wall_clock_s": get(ad, "RemoteWallClockTime"),
            "starts": get(ad, "NumJobStarts", 0),
        },
        "files": job_files(ad),
    }
    if status_code == JobStatus.HELD:
        out["hold"] = hold_info(ad)
    if status_code == JobStatus.COMPLETED:
        out["exit"] = {
            "code": get(ad, "ExitCode"),
            "by_signal": bool(get(ad, "ExitBySignal", False)),
            "signal": get(ad, "ExitSignal"),
            "completed_at": ts(get(ad, "CompletionDate")),
        }
    extra = site_attrs(ad, extra_attrs)
    if extra:
        out["site"] = extra
    return out


def compact_job(ad: classad.ClassAd) -> dict[str, Any]:
    """One row of a job listing."""
    status_code = get(ad, "JobStatus")
    row = {
        "job_id": job_id(ad),
        "owner": get(ad, "Owner"),
        "status": JOB_STATUS.get(status_code, f"unknown({status_code})"),
        "status_since": ts(get(ad, "EnteredCurrentStatus")),
        "command": command_line(ad),
        "submitted_at": ts(get(ad, "QDate")),
    }
    if status_code == JobStatus.RUNNING:
        row["running_on"] = get(ad, "RemoteHost")
    if status_code == JobStatus.HELD:
        code = get(ad, "HoldReasonCode")
        row["hold_code"] = code
        row["hold_meaning"] = HOLD_CODES.get(code, "see HoldReasonCode in the HTCondor manual")
    return row


def held_job(ad: classad.ClassAd) -> dict[str, Any]:
    """One row of the held-jobs report."""
    row = {
        "job_id": job_id(ad),
        "owner": get(ad, "Owner"),
        "command": command_line(ad),
        "held_since": ts(get(ad, "EnteredCurrentStatus")),
        "times_held": get(ad, "NumHolds"),
        **hold_info(ad),
    }
    files = split_files(get(ad, "TransferInput"))
    if files:
        row["transfer_input"] = files
    return row


def summarize_slot(ad: classad.ClassAd, extra_attrs: Iterable[str] = ()) -> dict[str, Any]:
    """One row of the per-slot pool listing."""
    row = {
        "name": get(ad, "Name"),
        "machine": get(ad, "Machine"),
        "state": get(ad, "State"),
        "activity": get(ad, "Activity"),
        "slot_type": get(ad, "SlotType"),
        "cpus": get(ad, "Cpus"),
        "memory_mb": get(ad, "Memory"),
        "disk_kb": get(ad, "Disk"),
        "gpus": get(ad, "GPUs"),
        "gpu_model": get(ad, "CUDADeviceName"),
        "claimed_by": get(ad, "RemoteOwner"),
        "load_avg": get(ad, "LoadAvg"),
        "since": ts(get(ad, "EnteredCurrentActivity")),
    }
    extra = site_attrs(ad, extra_attrs)
    if extra:
        row["site"] = extra
    return row
