import sys
from mcp.router import route_tool

def main():
    if len(sys.argv) < 2:
        print("Usage: python mcp/agent.py '<command>'")
        return

    command = sys.argv[1]
    tool = route_tool(command)

    if tool:
        result = tool()
        print(result)
    else:
        print("No matching tool.")

if __name__ == "__main__":
    main()
