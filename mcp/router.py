from mcp.tools import slurm_summary

def route_tool(command: str):
    command = command.lower()

    if "summary" in command or "report" in command:
        return slurm_summary.run_summary_tool
    return None
