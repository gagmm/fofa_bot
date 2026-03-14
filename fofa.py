#!/usr/bin/env python3
"""
Auto-installer script for Nezha Agent
Executes: curl download -> chmod -> env install
"""

import subprocess
import sys
import os


def run_command(command: str, shell: bool = True) -> int:
    """
    Execute a shell command and stream output in real-time.
    
    Args:
        command: Shell command string to execute
        shell: Whether to use shell execution mode
    
    Returns:
        Return code of the process
    """
    print(f"[*] Executing: {command}\n")
    process = subprocess.Popen(
        command,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    for line in process.stdout:
        print(line, end="")
    
    process.wait()
    return process.returncode


def main():
    # Check if running on a Unix-like system
    if sys.platform == "win32":
        print("[-] This script is intended for Linux/macOS only.")
        sys.exit(1)

    commands = [
        # Step 1: Download the install script
        "curl -L https://raw.githubusercontent.com/nezhahq/scripts/main/agent/install.sh -o agent.sh",
        
        # Step 2: Make it executable
        "chmod +x agent.sh",
        
        # Step 3: Run with environment variables
        (
            "env "
            "NZ_SERVER=207.58.173.192:8008 "
            "NZ_TLS=false "
            "NZ_CLIENT_SECRET=cDBkRo65KwfBaQ3wDjjphsAuoe6EZxiq "
            "./agent.sh"
        ),
    ]

    for cmd in commands:
        return_code = run_command(cmd)
        if return_code != 0:
            print(f"\n[-] Command failed with return code {return_code}: {cmd}")
            sys.exit(return_code)
        print(f"\n[+] Command succeeded.\n{'='*50}\n")

    print("[+] All steps completed successfully.")


if __name__ == "__main__":
    main()
