import json
import sys

def main():
    try:
        # Read the incoming tool call payload from stdin
        payload = json.loads(sys.stdin.read())
        tool_call = payload.get("toolCall", {})

        # Check if the agent is trying to run a shell command
        if tool_call.get("name") == "run_command":
            cmd = tool_call.get("args", {}).get("CommandLine", "").strip()

            # Identify if it's a git command
            if cmd.startswith("git ") or " git " in cmd:
                # List of forbidden mutating subcommands
                forbidden = ["checkout", "reset", "commit", "add", "apply", "restore", "clean", "rm", "push", "merge", "rebase"]

                # Check if any forbidden subcommand is used
                for f in forbidden:
                    # Check for whole-word match of the subcommand
                    if f" {f}" in cmd or f"\t{f}" in cmd:
                        print(json.dumps({
                            "decision": "deny",
                            "reason": f"SYSTEM BLOCK: Mutating git command '{f}' is strictly forbidden by workspace safety hooks."
                        }))
                        return

        # If it's safe, allow execution
        print(json.dumps({"decision": "allow"}))

    except Exception:
        # Failsafe: allow if script parsing errors
        print(json.dumps({"decision": "allow"}))

if __name__ == "__main__":
    main()