"""why_idle: live jobs no slot can match, jobs that are not idle, and made-up slots for
what a one-machine pool cannot show (busy slots, stranded GPUs, a START policy that
refuses, a schedd at its cap, users ahead in the fair-share order)."""

from __future__ import annotations

import getpass

import classad2 as classad
import htcondor2 as htcondor
import pytest

from condor_mcp import diagnose
from conftest import TAG, call, fake_priorities

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module")
def unmatchable():
    """Two idle jobs no slot can run: one requires an architecture no slot has, the other
    asks for more CPUs than any machine has."""
    schedd = htcondor.Schedd()
    common = {
        f"+{TAG}": "true",
        "executable": "/bin/sleep",
        "arguments": "60",
        "request_memory": "64MB",
        "request_disk": "10MB",
    }
    sparc = schedd.submit(htcondor.Submit({**common, "requirements": 'TARGET.Arch == "SPARC"'})).cluster()
    big = schedd.submit(htcondor.Submit({**common, "request_cpus": "999"})).cluster()
    try:
        yield {"sparc": sparc, "big": big}
    finally:
        schedd.act(htcondor.JobAction.Remove, f"ClusterId == {sparc} || ClusterId == {big}")


async def test_why_idle_no_slot_meets_requirements(unmatchable):
    out = await call("why_idle", cluster_id=unmatchable["sparc"])
    assert out["status"] == "idle" and out["meet_requirements"] == 0
    [sparc] = [c for c in out["requirements"] if "SPARC" in c["clause"]]
    assert sparc["rules_out"] == out["slots_considered"] >= 1
    assert 'TARGET.Arch == "SPARC" rules out every one' in out["reasons"][0]
    assert out["schedd_health"]["max_running"] >= 1
    assert out["priority"]["user"].startswith(f"{getpass.getuser()}@")


async def test_why_idle_asks_for_more_than_any_slot(unmatchable):
    out = await call("why_idle", cluster_id=unmatchable["big"])
    assert out["largest"]["cpus"]["requested"] == 999
    assert out["reasons"][0].startswith("The job asks for more than any slot has: 999 CPUs")
    assert any(c["clause"] == "TARGET.Cpus >= 999" and c["rules_out"] for c in out["requirements"])


async def test_why_idle_counts_its_user_as_waiting(monkeypatch, unmatchable):
    """A just-submitted job's user has no submitter ad yet but is waiting, so the rank
    sentence counts them among the users with idle jobs: never "ranks 4 of 3"."""
    from condor_mcp import pool

    [job] = htcondor.Schedd().query(f"ClusterId == {unmatchable['big']}", ["User"])
    fake_priorities(
        monkeypatch,
        records=[(job["User"], 900.0), ("a@d", 1.0), ("b@d", 2.0), ("c@d", 3.0)],
        submitters=[("a@d", 1), ("b@d", 1), ("c@d", 1)],
    )
    [real] = pool.query_slots([], "DynamicSlot =!= true")[:1]
    busy = classad.ClassAd()
    busy.update(real)
    busy.update({"PartitionableSlot": False, "State": "Claimed", "Cpus": 999, "TotalSlotCpus": 999})
    monkeypatch.setattr(pool, "query_slots", lambda projection, constraint=None: [busy])

    out = await call("why_idle", cluster_id=unmatchable["big"])
    assert out["priority"]["rank"] == 4
    assert "ranks 4 of 4 users with idle jobs" in out["reasons"][0], out["reasons"]


async def test_why_idle_held_job(live_jobs):
    out = await call("why_idle", cluster_id=live_jobs["held"])
    assert out["status"] == "held" and out["hold"]["code"] == 13
    assert "reasons" not in out


async def test_why_idle_running_job(live_jobs):
    out = await call("why_idle", cluster_id=live_jobs["running"])
    assert out["status"] == "running" and "running, on" in out["note"]


async def test_why_idle_finished_job(finished_job):
    out = await call("why_idle", cluster_id=finished_job)
    assert "left the queue" in out["note"]


# Made-up slots

SLOT_REQUIREMENTS = (
    "START && TARGET.RequestCpus <= MY.Cpus && TARGET.RequestMemory <= MY.Memory "
    "&& (TARGET.RequestGPUs is undefined || MY.GPUs >= TARGET.RequestGPUs)"
)


def slot(name, cpus, memory, gpus=0, totals=None, partitionable=True, state="Unclaimed", start="True"):
    total_cpus, total_memory, total_gpus = totals or (cpus, memory, gpus)
    ad = classad.ClassAd(
        {
            "Name": f"slot1@{name}",
            "Machine": name,
            "Arch": "X86_64",
            "State": state,
            "PartitionableSlot": partitionable,
            "Cpus": cpus,
            "Memory": memory,
            "Disk": 10**7,
            "GPUs": gpus,
            "TotalSlotCpus": total_cpus,
            "TotalSlotMemory": total_memory,
            "TotalSlotDisk": 10**7,
            "TotalSlotGPUs": total_gpus,
        }
    )
    ad["START"] = classad.ExprTree(start)
    ad["Requirements"] = classad.ExprTree(SLOT_REQUIREMENTS)
    return ad


def gpu_job():
    job = classad.ClassAd(
        {"Owner": "alice", "RequestCpus": 4, "RequestMemory": 8000, "RequestDisk": 100, "RequestGPUs": 1}
    )
    job["Requirements"] = classad.ExprTree(
        '(TARGET.Arch == "X86_64") && (TARGET.Cpus >= RequestCpus) && (TARGET.Memory >= RequestMemory) '
        "&& (TARGET.GPUs >= RequestGPUs)"
    )
    return job


def test_split_clauses():
    assert diagnose.split_clauses('a && (b || c) && d == "x && (y" && f(g && h)') == [
        "a",
        "b || c",
        'd == "x && (y"',
        "f(g && h)",
    ]
    assert diagnose.split_clauses("((a) && (b))") == ["a", "b"]
    assert diagnose.split_clauses("(a) || (b)") == ["(a) || (b)"]
    assert diagnose.split_clauses(r'a == "x\" && (" && b') == [r'a == "x\" && ("', "b"]


@pytest.mark.parametrize(
    "expr, clauses",
    [
        ("a || b && c", ["a || b && c"]),  # a || (b && c): no && term holds on its own
        ("a && b || c", ["a && b || c"]),
        ("a ? b && c : d", ["a ? b && c : d"]),
        ("a =?= b && c =!= d", ["a =?= b", "c =!= d"]),  # =?= is not ?:
        (
            '(TARGET.GPUs_DeviceName == "Tesla (16GB") && (TARGET.Cpus >= 1)',
            ['TARGET.GPUs_DeviceName == "Tesla (16GB"', "TARGET.Cpus >= 1"],
        ),
        ('("a)" == b)', ['"a)" == b']),
    ],
)
def test_split_respects_precedence_and_quotes(expr, clauses):
    assert diagnose.split_clauses(expr) == clauses


def test_match_with_a_parenthesis_in_a_string():
    """A ( inside a quoted string once crashed why_idle with a ClassAd parse error."""
    job = gpu_job()
    job["Requirements"] = classad.ExprTree('(TARGET.Arch == "X86_64 (64-bit") && (TARGET.Cpus >= 1)')
    matched = diagnose.match(job, [slot("cpu1", 8, 16000)])
    assert [c["clause"] for c in matched["requirements"]] == ['TARGET.Arch == "X86_64 (64-bit"', "TARGET.Cpus >= 1"]
    assert matched["requirements"][0]["rules_out"] == 1 and matched["requirements"][1]["rules_out"] == 0


def test_an_or_keeps_requirements_whole():
    """A slot with HasSpecial meets a || b && c, so b && c's parts must not be blamed."""
    job = gpu_job()
    job["Requirements"] = classad.ExprTree('TARGET.HasSpecial || TARGET.Arch == "X86_64" && TARGET.Cpus >= 64')
    special = slot("cpu1", 8, 16000)
    special["HasSpecial"] = True
    matched = diagnose.match(job, [special])
    assert matched["meet_requirements"] == 1 and len(matched["requirements"]) == 1


def test_claimed_partitionable_slot_is_not_free():
    """A claimed partitionable slot's leftovers belong to the schedd that claimed it."""
    matched = diagnose.match(gpu_job(), [slot("gpu1", 8, 16000, gpus=1, state="Claimed")])
    assert (matched["accept_the_job"], matched["free_now"], matched["stranded_gpus"]) == (1, 0, {})


def test_job_without_requirements():
    job = gpu_job()
    del job["Requirements"]
    matched = diagnose.match(job, [slot("gpu1", 8, 16000, gpus=1)])
    assert diagnose.reasons(matched) == ["The job has no Requirements expression, so no slot can match it."]


def test_one_cpu_too_many():
    job = gpu_job()
    job["RequestCpus"], job["RequestGPUs"] = 1, 0
    matched = diagnose.match(job, [slot("tiny", 0, 16000, partitionable=False)])
    assert diagnose.reasons(matched)[0].startswith("The job asks for more than any slot has: 1 CPU, but")


def test_one_stranded_gpu():
    matched = diagnose.match(gpu_job(), [slot("gpu1", 1, 2000, gpus=1, totals=(32, 64000, 4))])
    assert diagnose.reasons(matched)[1].startswith("1 GPU that fits the job is free on 1 machine")


def _schedd_ad(**attrs):
    ad = classad.ClassAd({"TotalSchedulerJobsRunning": 3, "MaxRunningSchedulerJobsPerOwner": 200})
    ad["StartSchedulerUniverse"] = classad.ExprTree(attrs.pop("start", "TotalSchedulerJobsRunning < 500"))
    ad.update(attrs)
    return ad


def test_draft_submit_and_why_idle_size_slots_alike(monkeypatch):
    """draft_submit's slot check and why_idle's match agree on which full-size slots fit."""
    from condor_mcp import pool, submit

    slots = [
        slot("gpu1", 1, 2000, gpus=2, totals=(32, 64000, 4)),  # mostly claimed, but fits when empty
        slot("cpu1", 64, 256000, partitionable=False, state="Claimed"),  # no GPU
        slot("gpu2", 8, 16000, gpus=1, start='TARGET.Owner == "bob"'),  # refuses alice
    ]
    monkeypatch.setattr(pool, "query_slots", lambda projection, constraint=None: slots)
    matched = diagnose.match(gpu_job(), slots)
    checked = submit.slot_check(gpu_job())
    assert (checked["fit_requirements"], checked["would_accept"]) == (2, 1)
    assert (checked["fit_requirements"], checked["would_accept"]) == (
        matched["meet_requirements"],
        matched["accept_the_job"],
    )
    assert checked["largest_slot"] == {"cpus": 64, "memory_mb": 256000, "disk_mb": 10**7 // 1024, "gpus": 4}
    assert matched["largest"]["gpus"]["largest_slot"] == 4


def test_universe_reasons():
    def job(universe, **attrs):
        return classad.ClassAd({"JobUniverse": universe, **attrs})

    assert diagnose.universe_reasons(job(5), _schedd_ad()) is None
    dagman = diagnose.universe_reasons(job(7), _schedd_ad())
    assert "scheduler-universe job (DAGMan runs this way)" in dagman[0]
    assert "cap of 200 running scheduler-universe jobs" in dagman[-1]
    closed = diagnose.universe_reasons(job(7), _schedd_ad(start="TotalSchedulerJobsRunning < 3"))
    assert "START_SCHEDULER_UNIVERSE policy (TotalSchedulerJobsRunning < 3) is false now" in closed[-1]
    assert "could not be read" in diagnose.universe_reasons(job(12), None)[-1]
    grid = diagnose.universe_reasons(job(9, GridResource="batch slurm"), None)
    assert "remote system batch slurm" in grid[0] and "no GridJobStatus" in grid[1]
    assert "reports it as IDLE" in diagnose.universe_reasons(job(9, GridJobStatus="IDLE"), None)[1]


async def test_why_idle_scheduler_universe():
    """A scheduler-universe job whose Requirements are false stays idle on the schedd."""
    schedd = htcondor.Schedd()
    submit = {
        f"+{TAG}": "true",
        "universe": "scheduler",
        "executable": "/bin/sleep",
        "arguments": "60",
        "requirements": "False",
    }
    cluster = schedd.submit(htcondor.Submit(submit)).cluster()
    try:
        out = await call("why_idle", cluster_id=cluster)
    finally:
        schedd.act(htcondor.JobAction.Remove, f"ClusterId == {cluster}")
    assert out["status"] == "idle" and "slots_considered" not in out and "priority" not in out
    assert "scheduler-universe job" in out["reasons"][0]
    assert out["reasons"][1] == "Its Requirements are always false, so the schedd will never start it."
    assert "cap of 200" in out["reasons"][2]


def test_stranded_gpus():
    """GPUs are free, but the CPUs and memory beside them are taken."""
    matched = diagnose.match(gpu_job(), [slot("gpu1", 1, 2000, gpus=2, totals=(32, 64000, 4))])
    assert (matched["meet_requirements"], matched["accept_the_job"], matched["free_now"]) == (1, 1, 0)
    assert matched["stranded_gpus"] == {"gpus": 2, "machines": ["gpu1"], "short_of": ["CPUs", "MB of memory"]}
    reasons = diagnose.reasons(matched)
    assert reasons[0].startswith("Every slot that could run the job (1 slot) is busy")
    assert (
        reasons[1]
        == "2 GPUs that fit the job are free on 1 machine, but with too few CPUs and MB of memory left to go with them."
    )


def test_free_now_and_users_ahead():
    matched = diagnose.match(gpu_job(), [slot("gpu1", 8, 16000, gpus=1, totals=(32, 64000, 4))])
    assert matched["free_now"] == 1 and matched["stranded_gpus"] == {}
    assert diagnose.reasons(matched) == [
        "1 slot could start the job now. It should start at one of the negotiator's next cycles."
    ]
    ahead = diagnose.reasons(matched, priority={"rank": 3}, users_competing=5)
    assert "2 users with idle jobs and a better fair-share priority may take them first" in ahead[0]


def test_busy_static_slot_and_rank():
    busy = slot("cpu1", 8, 16000, gpus=1, partitionable=False, state="Claimed")
    matched = diagnose.match(gpu_job(), [busy])
    assert (matched["accept_the_job"], matched["free_now"]) == (1, 0)
    assert "ranks 3 of 5 users with idle jobs" in diagnose.reasons(matched, priority={"rank": 3}, users_competing=5)[0]


def test_start_policy_refuses():
    matched = diagnose.match(gpu_job(), [slot("gpu1", 8, 16000, gpus=1, start='TARGET.Owner == "bob"')])
    assert (matched["meet_requirements"], matched["accept_the_job"]) == (1, 0)
    assert "START policy" in diagnose.reasons(matched)[0]


def test_no_clause_rules_out_every_slot():
    """Each slot fails a different clause, so the reason names the clause that rules out most."""
    slots = [slot("small1", 2, 64000, gpus=1), slot("small2", 2, 64000, gpus=1), slot("nogpu", 8, 64000)]
    matched = diagnose.match(gpu_job(), slots)
    assert matched["meet_requirements"] == 0
    assert "The one that rules out the most is TARGET.Cpus >= 4 (2 of 3 slots)" in diagnose.reasons(matched)[0]


def test_schedd_limits():
    matched = diagnose.match(gpu_job(), [slot("gpu1", 8, 16000, gpus=1)])
    capped = {
        "name": "s1",
        "max_running": 10,
        "note": "At its MaxJobsRunning cap: ...",
        "load": "overloaded",
        "duty_cycle": 0.97,
    }
    reasons = diagnose.reasons(matched, schedd=capped)
    assert "s1 is at its cap of 10 running jobs" in reasons[1]
    assert "s1 is overloaded (duty cycle 0.97)" in reasons[2]
