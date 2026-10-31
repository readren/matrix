# 🛠 Metals & Antigravity Troubleshooting Guide

This guide covers common fixes for the Scala Presentation Compiler (PC), Inlay Hints, and Signature Help when working with large files and path-dependent types.

## 1. Essential "First Aid" Commands
If the IDE stops showing types or parameter hints, try these in order:
* **Clean & Compile:** Run `sbt clean compile` in your terminal.
* **Restart Metals:** `Ctrl+Shift+P` -> `Metals: Restart server`.
* **Clear Caches:** `Ctrl+Shift+P` -> `Metals: Clear caches and reimport project`.

---

## 2. Fixing Signature Help (`Ctrl+Shift+Space`)
If the shortcut doesn't trigger the parameter popup:
1. **Check Keybindings:** Ensure `editor.action.triggerParameterHints` is bound to `Ctrl+Shift+Space`.
2. **Troubleshoot Hijacks:** Run `Developer: Toggle Keyboard Shortcuts Troubleshooting`. If you press the keys and nothing logs, an OS app (like PowerToys, AMD/Nvidia overlay, or a Window Manager) is stealing the shortcut.
3. **Manual Trigger:** If it works via the Command Palette but not the keys, it is definitely a keyboard conflict.

---

## 3. Inlay Hints (The `Ctrl+Alt` Toggle)
If hints don't appear when holding the modifier:
* **Enable in Settings:** Ensure `editor.inlayHints.enabled` is set to `offUnlessPressed`.
* **Set the Key:** Ensure `editor.inlayHints.helpModifierKey` is set to `ctrlAlt`.
* **Metals Config:** Ensure these are enabled in your User Settings:
  ```json
  "metals.inlayHints.parameterNames.enable": true,
  "metals.inlayHints.inferredTypes.enable": true
  ```

---

## 4. Solving `ArrayIndexOutOfBoundsException` / Length 0
If the Metals Doctor reports crashes in specific files (especially large files with path-dependent types):

### Increase JVM Memory
Add these to `metals.serverProperties` in your settings:
```json
"metals.serverProperties": [
    "-Xmx4G", // default is 2G. Usually this alone solves everything.
    "-Dmetals.pc-timeout=20s", // default is 10s
    "-Dmetals.enable-best-effort=true" // default is false
]
```
* **-Xmx4G**: Gives the Presentation Compiler more room to map large files.
* **pc-timeout**: Prevents the compiler from giving up too early on complex types.
* **best-effort**: Allows Metals to provide partial info even if the full tree fails.

---

## 5. Maintenance
### Clearing Old Doctor Reports
Metals stores error logs in a hidden folder. To clear the "collapsed list" at the bottom of the Doctor:
1. Close the IDE.
2. Delete the directory: `.metals/reports/`.
3. Restart the IDE.

### The "Ghost Character" Reset
If one specific file is acting up:
1. `Ctrl+A` -> `Ctrl+X` (Cut everything).
2. Save the empty file.
3. `Ctrl+V` -> Save again.
*This forces a total re-indexing of the file's character offsets.*

---

## 8. The "Version Switch" Cleanup
If you downgrade or upgrade Scala versions and the Doctor reports plugin errors:
1. **Close the IDE.**
2. **Delete `project/metals.sbt`**: This removes stale plugin injections.
3. **Delete `.bloop/` and `.metals/`**: This clears the compiled analysis and indexes.
4. **Restart & Import**: Let Metals recreate these folders from scratch. 
   *Note: This will trigger a full re-index, which may take a few minutes for large projects.*
