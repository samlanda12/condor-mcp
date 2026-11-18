"""user_priority: the negotiator's fair-share records joined with the submitter ads."""

from __future__ import annotations

import getpass

import pytest

from conftest import call

pytestmark = pytest.mark.anyio
ME = getpass.getuser()


async def test_user_priority_me(live_jobs):
    out = await call("user_priority")
    assert out["as_of"]
    assert out["users"], out
    for row in out["users"]:
        assert row["user"].split("@")[0].split(".")[-1] == ME
    plain = next(row for row in out["users"] if row["user"].startswith(f"{ME}@"))
    assert plain["effective_priority"] == pytest.approx(plain["real_priority"] * plain["priority_factor"], rel=1e-2)
    # Submitter ads trail the queue by up to SCHEDD_INTERVAL, so only their shape is checked
    assert set(plain["jobs"]) == {"idle", "running", "held"}
    assert plain["rank"] >= 1


async def test_user_priority_all(live_jobs):
    out = await call("user_priority", owner="all")
    assert any(row["user"].startswith(f"{ME}@") for row in out["users"])
    for row in out["users"]:
        assert sum(row["jobs"].values()) > 0, row  # only users with jobs in a queue
    priorities = [row["effective_priority"] for row in out["users"] if "effective_priority" in row]
    assert priorities == sorted(priorities)


async def test_user_priority_unknown_user():
    out = await call("user_priority", owner="condor-mcp-nobody")
    assert out["users"] == []
    assert "no record of condor-mcp-nobody" in out["note"]


def test_negotiator_answer_is_reused(monkeypatch):
    from condor_mcp import pool

    monkeypatch.setattr(pool, "_priorities", None)
    ads, read_at = pool.accountant_ads()
    assert pool.accountant_ads()[1] == read_at
    monkeypatch.setattr(pool, "_priorities", (read_at - pool.PRIORITY_MAX_AGE - 1, ads))
    assert pool.accountant_ads()[1] > read_at - pool.PRIORITY_MAX_AGE - 1


async def test_rank_counts_only_competing_users_ahead(monkeypatch):
    """Faked records: rank counts users with idle or running jobs and a better priority,
    group records are left out, and a user's accounting-group records are theirs."""
    import classad2 as classad

    from condor_mcp import pool

    def record(name, priority, group=False):
        return classad.ClassAd({"Name": name, "Priority": priority, "PriorityFactor": 1000.0,
                                "IsAccountingGroup": group, "WeightedAccumulatedUsage": 7200.0})

    def submitter(name, idle=0, running=0, held=0):
        return classad.ClassAd({"Name": name, "IdleJobs": idle, "RunningJobs": running, "HeldJobs": held})

    monkeypatch.setattr(pool, "accountant_ads", lambda: ([
        record("alice@d", 900.0),
        record("group_x.alice@d", 300.0),
        record("bob@d", 200.0),
        record("carol@d", 100.0),
        record("dave@d", 50.0),
        record("<none>", 1.0, group=True),
    ], 0.0))
    monkeypatch.setattr(pool, "query_submitters", lambda: [
        submitter("alice@d", idle=5),
        submitter("bob@d", running=2),
        submitter("bob@d", idle=1),  # a second schedd
        submitter("carol@d", held=3),  # held only, so not competing
        submitter("erin@d", idle=1),  # no record yet
    ])

    out = await call("user_priority", owner="alice")
    assert out["users_competing"] == 3
    assert [(row["user"], row["rank"]) for row in out["users"]] == [("group_x.alice@d", 2), ("alice@d", 2)]
    assert out["users"][1]["usage_hours"] == 2.0

    out = await call("user_priority", owner="all")
    users = {row["user"]: row for row in out["users"]}
    assert list(users) == ["carol@d", "bob@d", "alice@d", "erin@d"]  # dave has no jobs queued
    assert users["bob@d"]["jobs"] == {"idle": 1, "running": 2, "held": 0}
    assert users["bob@d"]["rank"] == 1
    assert "rank" not in users["erin@d"] and "no usage record" in users["erin@d"]["note"]
