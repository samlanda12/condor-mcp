"""Why an idle job is not running: its ad matched against the pool's slot ads, the way
condor_q -better-analyze does, plus the limits matching does not show (the schedd's
running cap, the user's place in the fair-share order, GPUs with no CPUs left beside them).

Pure functions over ClassAds, so they can be tested with made-up slots. Slots are
partitionable and static slot ads; dynamic slots are left out, since their resources
belong to the partitionable slot they came from and to the job that claimed them.
"""

from __future__ import annotations

from typing import Any

import classad2 as classad

from condor_mcp.classads import get

# Slot attribute, its full size on a partitionable slot, the job's request, and the unit
RESOURCES = [
    ("Cpus", "TotalSlotCpus", "RequestCpus", "CPUs"),
    ("Memory", "TotalSlotMemory", "RequestMemory", "MB of memory"),
    ("Disk", "TotalSlotDisk", "RequestDisk", "KB of disk"),
    ("GPUs", "TotalSlotGPUs", "RequestGPUs", "GPUs"),
]


def split_clauses(expr: str) -> list[str]:
    """The top-level && terms of an expression, outer parentheses removed. Quotes and
    nested parentheses are respected, so a || inside a term stays in that term."""
    expr = _strip_parens(expr.strip())
    clauses, depth, start, i, quote = [], 0, 0, 0, False
    while i < len(expr):
        ch = expr[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == '"':
                quote = False
        elif ch == '"':
            quote = True
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and expr.startswith("&&", i):
            clauses.append(expr[start:i])
            start = i + 2
            i += 1
        i += 1
    clauses.append(expr[start:])
    return [_strip_parens(c.strip()) for c in clauses if c.strip()]


def _strip_parens(term: str) -> str:
    """term without parentheses that wrap all of it."""
    while term.startswith("(") and term.endswith(")"):
        depth = 0
        for i, ch in enumerate(term):  # quotes are rare enough here that a stray ( in one only costs the strip
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(term) - 1:
                return term
        term = term[1:-1].strip()
    return term


def _copy(ad: classad.ClassAd) -> classad.ClassAd:
    out = classad.ClassAd()
    out.update(ad)
    return out


def full_size(slot: classad.ClassAd) -> classad.ClassAd:
    """A partitionable slot as if none of its resources were claimed, for "could this
    machine ever run the job". A static slot is returned as it is."""
    if not get(slot, "PartitionableSlot", False):
        return slot
    out = _copy(slot)
    for attr, total, _, _ in RESOURCES:
        if total in slot:
            out[attr] = slot[total]
    return out


def free_now(slot: classad.ClassAd) -> bool:
    """Whether the slot has resources no job holds: a partitionable slot's leftovers, or
    an unclaimed static slot."""
    return bool(get(slot, "PartitionableSlot", False)) or get(slot, "State") == "Unclaimed"


def _with_requirements(job: classad.ClassAd, expr: str) -> classad.ClassAd:
    probe = _copy(job)
    probe["Requirements"] = classad.ExprTree(expr)
    return probe


def match(job: classad.ClassAd, slots: list[classad.ClassAd]) -> dict[str, Any]:
    """Match the job against each slot at full size and as it is now.

    Returns counts of slots that meet the job's requirements, that also accept the job
    (their START policy and resource limits), and that could start it now; one entry per
    requirement clause with how many slots it rules out; the largest of each resource any
    slot has, against what the job asks for; and GPUs free on a slot that matches but
    has too few CPUs, memory or disk left to go with them."""
    sized = [full_size(slot) for slot in slots]
    requirements = job.lookup("Requirements") if "Requirements" in job else None
    clauses = []
    if requirements is not None:
        for clause in split_clauses(str(requirements)):
            probe = _with_requirements(job, clause)
            clauses.append({
                "clause": str(job.flatten(classad.ExprTree(clause))),
                "rules_out": sum(1 for slot in sized if not slot.matches(probe)),
            })
    meets = [i for i, slot in enumerate(sized) if slot.matches(job)]
    accepts = [i for i in meets if job.matches(sized[i])]
    now = [i for i in accepts if free_now(slots[i]) and slots[i].symmetricMatch(job)]

    largest = {}
    for attr, total, request, unit in RESOURCES:
        wanted = get(job, request, 0) or 0
        sizes = [get(slot, attr, 0) or 0 for slot in sized]
        if wanted:
            largest[attr.lower()] = {"requested": wanted, "largest_slot": max(sizes, default=0), "unit": unit}

    stranded: dict[str, Any] = {}
    gpus = get(job, "RequestGPUs", 0) or 0
    if gpus:
        short = []
        for i in accepts:
            slot = slots[i]
            if (get(slot, "GPUs", 0) or 0) < gpus or not free_now(slot):
                continue
            missing = [unit for attr, _, request, unit in RESOURCES[:3]
                       if (get(slot, attr, 0) or 0) < (get(job, request, 0) or 0)]
            if missing:
                short.append((slot, missing))
        if short:
            stranded = {
                "gpus": sum(get(slot, "GPUs", 0) or 0 for slot, _ in short),
                "machines": sorted({get(slot, "Machine") or get(slot, "Name") for slot, _ in short}),
                "short_of": sorted({unit for _, missing in short for unit in missing}),
            }
    return {
        "slots_considered": len(slots),
        "meet_requirements": len(meets),
        "accept_the_job": len(accepts),
        "free_now": len(now),
        "requirements": clauses,
        "largest": largest,
        "stranded_gpus": stranded,
    }


def _slots(n: int) -> str:
    return f"{n} slot" if n == 1 else f"{n} slots"


def _amount(value: Any) -> str:
    return f"{round(float(value)):,}"


def reasons(
    matched: dict[str, Any],
    schedd: dict[str, Any] | None = None,
    priority: dict[str, Any] | None = None,
    users_competing: int = 0,
) -> list[str]:
    """Plain-language reasons the job is idle, the most likely first. schedd is a
    classads.schedd_health row and priority a user_priority row for the job's user."""
    out = []
    n = matched["slots_considered"]
    too_big = [f"{_amount(v['requested'])} {v['unit']}, but the largest slot has {_amount(v['largest_slot'])}"
               for v in matched["largest"].values() if v["requested"] > v["largest_slot"]]
    if too_big:
        out.append("The job asks for more than any slot has: " + "; ".join(too_big) + ".")
    if matched["meet_requirements"] == 0:
        clauses = sorted(matched["requirements"], key=lambda c: c["rules_out"], reverse=True)
        everywhere = [c["clause"] for c in clauses if c["rules_out"] == n]
        if len(everywhere) == 1:
            out.append(f"No slot meets the job's requirements: {everywhere[0]} rules out every one of "
                       f"the {_slots(n)}.")
        elif everywhere:
            out.append(f"No slot meets the job's requirements: each of {'; '.join(everywhere)} rules out "
                       f"every one of the {_slots(n)}.")
        elif clauses:
            out.append(f"No slot meets all of the job's requirements at once. The one that rules out "
                       f"the most is {clauses[0]['clause']} ({clauses[0]['rules_out']} of {_slots(n)}).")
        else:
            out.append(f"None of the {_slots(n)} meets the job's requirements.")
    elif matched["accept_the_job"] == 0:
        out.append(f"The job's requirements are met by {_slots(matched['meet_requirements'])}, but none of "
                   "them accepts the job: their START policy or resource limits turn it away.")
    elif matched["free_now"] == 0:
        line = (f"Every slot that could run the job ({_slots(matched['accept_the_job'])}) is busy, so it "
                "waits for one to free up.")
        if priority and priority.get("rank", 1) > 1:
            line += (f" Its user ranks {priority['rank']} of {users_competing} users with jobs waiting or "
                     "running, and the ones ahead are offered free slots first.")
        out.append(line)
    else:
        line = f"{_slots(matched['free_now'])} could start the job now."
        if priority and priority.get("rank", 1) > 1:
            line += (f" Users ahead of its user in the fair-share order ({priority['rank'] - 1} of them) "
                     "may take them first.")
        else:
            line += " It should start at one of the negotiator's next cycles."
        out.append(line)
    stranded = matched["stranded_gpus"]
    if stranded:
        machines = len(stranded["machines"])
        out.append(f"{stranded['gpus']} GPUs that fit the job are free on {machines} "
                   f"{'machine' if machines == 1 else 'machines'}, but with too few "
                   f"{' and '.join(stranded['short_of'])} left to go with them.")
    if schedd and "MaxJobsRunning" in schedd.get("note", ""):
        out.append(f"The job's schedd {schedd['name']} is at its cap of {schedd['max_running']} running "
                   "jobs, so none of its jobs start until running ones end.")
    if schedd and schedd.get("load") == "overloaded":
        out.append(f"The job's schedd {schedd['name']} is overloaded (duty cycle {schedd['duty_cycle']}), "
                   "so it is slow to start jobs.")
    return out
