"""The MCP server: MCP tools over an HTCondor pool."""

from __future__ import annotations

from condor_mcp.app import mcp

# Each import registers that module's tools on mcp, and clients list them in this order.
# isort: off
import condor_mcp.tools.jobs
import condor_mcp.tools.pool
import condor_mcp.tools.diagnosis
import condor_mcp.tools.submit
import condor_mcp.tools.actions  # noqa: F401
# isort: on


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
