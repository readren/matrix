# Cross-OS Scala Development Guide: Gusta Edition

**Architecture:** Human-Centric IntelliJ (Windows) + AI-Centric Antigravity (WSL)

## 1. Git Worktree Architecture

To fix permission loops and performance lag, we move the AI’s working directory into the Linux filesystem while keeping it synced with your Windows project.

### Initialization (Run in WSL/Ubuntu Terminal)

1. **Navigate to the Windows source:**
   ```bash
   cd /mnt/c/Projects/tools/matrix-ai
   ```
2. **Create the Linux-native Worktree:**
   ```bash
   git worktree add ~/matrix-wsl
   ```
3. **Configure Git for Cross-OS Stability:**
   ```bash
   git config core.autocrlf input
   git config core.filemode false
   ```

---

## 2. Antigravity IDE Configuration (The AI Sandbox)

Running Antigravity through the WSL bridge allows the AI Agent to use **Bash**, which correctly handles the "Always Proceed" signals that PowerShell 5.1 fails to send.

* **Connection Method:** Do **not** install the Linux binary. Use your Windows Antigravity app and press `Ctrl + Shift + P` -> **`Remote-WSL: Connect to WSL`**.
* **Target Folder:** Open `~/matrix-wsl`.
* **Verification:** Ensure the bottom-left corner of the IDE displays **`[WSL: Ubuntu]`**.
* **The "Silent Agent" Fix:** 1. Open **`User Settings (JSON)`**.
    2. Set `"terminal.integrated.shellIntegration.enabled": false`.
    3. This prevents Windows security hooks from intercepting the AI's commands.

---

## 3. IntelliJ Configuration (The Human Office)

Since you prefer coding in IntelliJ on Windows, ensure it doesn't conflict with the AI’s Linux environment.

* **Line Endings:** Go to `File -> Settings -> Editor -> Code Style` -> Set **Line separator** to **Unix (\n)**.
* **Integrated Terminal:** Use **PowerShell 7** (`pwsh.exe`) instead of the legacy v5.1.
    * Check version with: `$PSVersionTable`.
* **Build Isolation:** Ensure `.gitignore` includes `target/`, `.bloop/`, and `.metals/`.
    * *Warning:* Sharing these folders across the OS boundary will cause "Class Not Found" errors.

---

## 4. Environment & Shell Prep

Both environments need their own independent Scala toolchains.

### WSL (Ubuntu) Setup

```bash
# Install Java and SDKMAN
sudo apt update && sudo apt install default-jdk
curl -s "https://get.sdkman.io" | bash

# After restarting terminal, install Scala tools
sdk install sbt
sdk install scala 3.x.x
```

### Windows Setup

* **PowerShell 7:** Install via Microsoft Store or `winget install Microsoft.PowerShell`.
* **Antigravity Config Path:** `C:\Users\gusta\.antigravity\config.json`.

---

## 5. Summary Workflow Table

| Feature               | Human (IntelliJ)      | AI (Antigravity)               |
|:----------------------|:----------------------|:-------------------------------|
| **Operating System**  | Windows 11            | WSL2 (Ubuntu)                  |
| **Filesystem**        | NTFS (Windows Drive)  | Ext4 (Linux Root)              |
| **Shell**             | PowerShell 7          | Bash                           |
| **Primary Goal**      | Design & Architecture | Autonomous Refactoring & Tests |
| **Permission Status** | Manual / UAC Enabled  | **Always Proceed / Automatic** |

### Final Troubleshooting Tip

If Antigravity still asks for permission in WSL:

1. Check that you are not opening the project via a network path like `\\wsl.localhost\`.
2. Ensure you have run `chmod +x` on any scripts the AI needs to execute.
3. Restart the Antigravity "Server" inside WSL (via the Command Palette).