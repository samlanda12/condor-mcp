# condor-mcp

An [MCP](https://modelcontextprotocol.io/) server that puts an [HTCondor](https://htcondor.readthedocs.io/en/latest/) pool behind a
natural-language interface. It lets an AI assistant answer questions like "why isn't my job running?" by calling tools that query the pool through the [htcondor2 Python bindings](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/).

Built with [FastMCP](https://gofastmcp.com/) ([repo](https://github.com/jlowin/fastmcp)) for Syracuse University's OrangeGrid
cluster. Example submit files to test against live in
[OrangeGridExamples](https://github.com/SyracuseUniversity/OrangeGridExamples).

## Requirements

- A submit host: a machine where `condor_q` works and that has an HTCondor config pointing
  at the pool. The server runs as the user whose jobs it should see, with that user's
  credentials. It does not need root.
- Python 3.12 or newer and [uv](https://docs.astral.sh/uv/). The HTCondor Python bindings
  are pulled in by `uv sync` and are pinned to match the pool's daemon version in
  [pyproject.toml](pyproject.toml). Change the pin if your pool runs something else.
  The bindings ship as Linux-only wheels, so `uv sync` fails on macOS and Windows; run
  the server on the submit host itself.

## Install

```sh
git clone https://github.com/samlanda12/condor-mcp.git
cd condor-mcp
uv sync
```

Check it can see the pool:

```sh
uv run python -c "import htcondor2 as h; print(h.Schedd().query(projection=['ClusterId'], limit=1))"
```

## Running it

The server speaks MCP over stdio. Point any MCP client at this command, run from the
repo directory:

```sh
uv run condor-mcp
```

For Claude Code, drop a `.mcp.json` in the repo root:

```json
{
  "mcpServers": {
    "condor": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/condor-mcp", "condor-mcp"]
    }
  }
}
```

Other clients take the same command and args in their own config format. Put any
`CONDOR_MCP_*` settings (below) in the client's `env` block.

## Tools

All of these are read-only. Jobs that have finished and left the queue are invisible
to them; job history comes in a later stage.

| Tool | What it answers |
| --- | --- |
| `job_details(cluster_id, proc_id=0)` | Everything about one job: status, where it runs, requested vs used resources, input/output files, and the hold reason in plain language if it is held. |
| `list_jobs(owner="me", status=None, limit=50)` | One line per job plus a count of the owner's jobs by status. `owner` is `me`, `all`, or a username. |
| `hold_reasons(owner="me")` | Every held job with HTCondor's message, the hold code, what the code means, and a tally by code. |
| `pool_status(detail=False)` | Machines, slots by state, CPUs / memory / GPUs total and available, GPUs by model, largest free CPU block. `detail=True` adds one row per slot. |

Code layout: [classads.py](condor_mcp/classads.py) turns ClassAds into dicts and
holds the hold-code table, [pool.py](condor_mcp/pool.py) reads the settings, knows
where the pool is and wraps connection errors, [server.py](condor_mcp/server.py) defines the
tools.

## Configuration

With no settings the server uses the local HTCondor config: the default collector, the
local schedd, and the user it runs as. Override with environment variables. They are
read once at startup, and an empty value is the same as unset.

| Variable | Default | What it does |
| --- | --- | --- |
| `CONDOR_MCP_COLLECTOR` | from the HTCondor config | Collector to talk to, as `host[:port]`. |
| `CONDOR_MCP_SCHEDDS` | local schedd | Schedds to query, comma-separated. Results are tagged with the schedd name. |
| `CONDOR_MCP_USER` | the user running the server | Who `owner="me"` refers to. |
| `CONDOR_MCP_SLOT_CONSTRAINT` | `True` | Only slots matching this ClassAd expression count in `pool_status`. |
| `CONDOR_MCP_JOB_ATTRS` | none | Extra job attributes to return, comma-separated. |
| `CONDOR_MCP_SLOT_ATTRS` | none | Extra slot attributes to return from `pool_status(detail=True)`, comma-separated. |
| `CONDOR_MCP_CALL_LOG` | off | File to log each tool call to. Handy during a demo. |

GPU attributes are always fetched. On a pool without GPUs they come back empty, which is
expected.

## Tests

The tests run against a live pool and need permission to submit. They submit a sleep job
and a job that goes on hold with a missing input file, wait for both, check every tool,
and remove only the jobs they submitted (tagged `CondorMcpTest`). Don't run them against
a production schedd you don't own. They also assume a small development pool: a single
execute machine with no GPUs, as in the container below. On a bigger pool the server
works but the `pool_status` tests fail.

```sh
uv run pytest
```

## Developing

Any HTCondor pool you can submit to works for development, including a single-machine
one such as the [htcondor/mini](https://hub.docker.com/r/htcondor/mini) container. Whatever
you use, run the server and the tests inside it as a user allowed to submit.
