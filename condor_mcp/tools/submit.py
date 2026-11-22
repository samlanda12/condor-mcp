"""Tools that write and queue submit descriptions."""

from __future__ import annotations

from typing import Annotated, Any

import htcondor2 as htcondor
from fastmcp.exceptions import ToolError
from pydantic import Field

from condor_mcp import classads, pool, submit, worker
from condor_mcp.app import READ_ONLY, WRITE, mcp, no_slots
from condor_mcp.classads import get
from condor_mcp.pool import CONDOR_ERRORS, log, settings
from condor_mcp.worker import NoAnswer


@mcp.tool(annotations=READ_ONLY, tags={"jobs", "read-only"})
def draft_submit(
    executable: Annotated[str, Field(description="Program or script to run.")],
    arguments: Annotated[
        str, Field(description="Its command-line arguments. $(item) is the queue item, $(ProcId) the job number.")
    ] = "",
    input_files: Annotated[
        list[str] | None,
        Field(
            description="Files to copy to the execute machine before the job starts. $(item) stands for the queue item."
        ),
    ] = None,
    output_files: Annotated[
        list[str] | None,
        Field(
            description="Files or directories to copy back when the job ends. "
            "Empty means every new file in the job's scratch directory."
        ),
    ] = None,
    cpus: Annotated[int, Field(ge=1)] = 1,
    memory_mb: Annotated[int, Field(ge=1)] = 1024,
    disk_mb: Annotated[int | None, Field(ge=1, description="Scratch disk. Omit to let HTCondor estimate it.")] = None,
    gpus: Annotated[int, Field(ge=0)] = 0,
    queue: Annotated[
        int | list[str], Field(description="How many copies to queue, or a list of items to queue one job each for.")
    ] = 1,
    extra: Annotated[
        dict[str, str] | None,
        Field(
            description="Other submit commands, verbatim, such as "
            '{"requirements": "HasAVX2", "+WantCloudBurst": "\\"orangegrid-2026\\""}. '
            "They come last and override the generated commands."
        ),
    ] = None,
) -> dict[str, Any]:
    """Turn a structured job description into an HTCondor submit file and check
    it without queuing anything: the text is parsed, run through condor_submit's
    dry run, and its requirements are matched against the pool's slots at full
    size. Returns the submit text to show the user, how many jobs it would
    queue, and warnings: a missing executable or input file, a typo in an extra
    command, or a request no machine in the pool could ever satisfy. Use
    queue=["a.csv", "b.csv"] with $(item) in arguments or input_files to run
    one job per item. The job runs from the server's work directory; its log,
    stdout and stderr land there named after the job id, and relative paths
    are taken relative to it."""
    log.info("draft_submit executable=%s queue=%s", executable, queue)
    workdir = settings.workdir
    executable = submit.resolve(executable, workdir)
    input_files = [submit.resolve(f, workdir) for f in input_files or []]
    text = submit.compose(
        executable=executable,
        arguments=arguments,
        input_files=input_files,
        output_files=output_files or [],
        cpus=cpus,
        memory_mb=memory_mb,
        disk_mb=disk_mb,
        gpus=gpus,
        queue=queue,
        extra=extra or {},
        workdir=workdir,
    )
    if reason := submit.runs_commands(text):
        raise ToolError(f"The draft is not accepted: {reason}")
    warnings = submit.check_files(executable, input_files, queue, workdir)
    out: dict[str, Any] = {
        "submit_text": text,
        "work_dir": str(workdir),
        "jobs": 0,
        "submittable": False,
        "requirements": None,
        "slots": None,
        "warnings": warnings,
    }
    if problem := submit.parse(text):
        warnings.append(problem)
        return out
    ads, messages = submit.dry_run(text)
    warnings.extend(messages)
    if not ads:
        return out
    out["jobs"] = len(ads)
    out["submittable"] = True
    out["requirements"] = str(ads[0].get("Requirements"))
    try:
        slots = submit.slot_check(ads[0])
    except ToolError as exc:
        warnings.append(f"Could not check the requirements against the pool: {exc}")
        return out
    out["slots"] = slots
    big = slots["largest_slot"]
    if slots["slots"] == 0:
        warnings.append(f"Could not check the requirements against the pool: {no_slots()}")
    elif slots["fit_requirements"] == 0:
        ad = ads[0]  # the effective requests, since extra can override the arguments
        warnings.append(
            "No slot in the pool satisfies the job's requirements, even when empty. The "
            f"job asks for {get(ad, 'RequestCpus')} CPUs, {get(ad, 'RequestMemory')} MB and "
            f"{get(ad, 'RequestGPUs', 0)} GPUs; no slot has more than {big['cpus']} CPUs, "
            f"{big['memory_mb']} MB or {big['gpus']} GPUs. Check the requirements line too."
        )
    elif slots["would_accept"] == 0:
        warnings.append(
            f"{slots['fit_requirements']} slots satisfy the job's requirements but none "
            "would accept it under its own START policy, likely because the dry-run ad has no Owner."
        )
    log.info("  -> %d jobs, %d warnings", out["jobs"], len(warnings))
    return out


@mcp.tool(annotations=WRITE, tags={"jobs", "write"})
def submit_job(
    submit_text: Annotated[
        str | None,
        Field(
            description="A complete submit description, such as the submit_text from draft_submit "
            "once the user has approved it."
        ),
    ] = None,
    submit_file: Annotated[
        str | None,
        Field(
            description="Instead of text: the path of a submit file on the submit host. "
            "Its relative paths are taken relative to its own directory."
        ),
    ] = None,
    dry_run: Annotated[bool, Field(description="Queue nothing; return the job ads HTCondor would create.")] = False,
) -> dict[str, Any]:
    """Queue jobs from a submit description, given as text or as the path of a
    submit file on the submit host. Jobs run as the user this server runs as,
    and go to the first schedd in CONDOR_MCP_SCHEDDS, or the local one if unset.
    The description first goes through condor_submit's dry run and is refused
    if that reports an error. With dry_run=True nothing is queued and the
    generated job ads come back instead, one row per job. include lines, a
    second queue statement, and queue statements with a | or a macro are
    refused. Returns the cluster id, the number of jobs, and one list_jobs row
    per job; follow a job with job_details."""
    log.info("submit_job file=%s dry_run=%s text=%s", submit_file, dry_run, submit_text is not None)
    text, file_dir = submit.load(submit_text, submit_file)
    if reason := submit.runs_commands(text):
        raise ToolError(f"Refusing to submit: {reason}")
    if problem := submit.parse(text):
        raise ToolError(problem)
    description = htcondor.Submit(text)
    if file_dir is None:
        try:
            settings.workdir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ToolError(f"Could not create the work directory {settings.workdir}: {exc}") from exc
    default_dir = file_dir or settings.workdir
    base = submit.prepare(description, default_dir)
    # The original text still has a relative initialdir, so run from where condor_submit would.
    ads, warnings = submit.dry_run(text, cwd=default_dir)
    errors = [m for m in warnings if m.startswith("ERROR")]
    if errors or not ads:
        reported = " ".join(errors or warnings) or "no job ads"
        raise ToolError(f"Refusing to submit, condor_submit reported: {reported}")
    if dry_run:
        rows = [submit.dry_run_row(ad) for ad in submit.complete_ads(ads)]
        log.info("  -> dry run, %d jobs", len(rows))
        return {"dry_run": True, "jobs": len(rows), "submit_dir": str(base), "job_ads": rows, "warnings": warnings}

    name, schedd = next(pool.schedds())
    try:
        # As text plus what prepare() pinned: a Submit object cannot be sent to the process
        # that makes the call, and str(description) loses an inline queue item list
        pinned = {key: description[key] for key in ("initialdir", "executable") if key in description}
        result = pool.call(worker.schedd_submit, schedd, text, pinned)
    except NoAnswer as exc:
        raise ToolError(
            f"The schedd ({name}) did not answer the submit: {exc}. The jobs may or may not "
            "have been queued; check with list_jobs before submitting again."
        ) from exc
    except (htcondor.HTCondorException, RuntimeError, ValueError) as exc:
        raise ToolError(f"The schedd ({name}) refused the submit: {exc}") from exc
    cluster = result.cluster()
    try:
        queued = pool.call(worker.schedd_query, schedd, f"ClusterId == {cluster}", classads.JOB_LIST_PROJECTION, -1)
    except CONDOR_ERRORS as exc:
        raise ToolError(f"Submitted cluster {cluster} but could not read it back: {exc}") from exc
    queued.sort(key=lambda ad: get(ad, "ProcId", 0))
    out: dict[str, Any] = {
        "cluster_id": cluster,
        "jobs": result.num_procs(),
        "submit_dir": str(base),
        "job_list": [classads.compact_job(ad) for ad in queued],
        "warnings": warnings,
    }
    pool.tag_schedd(out, name)
    log.info("  -> cluster %d, %d jobs", cluster, result.num_procs())
    return out
