' Run the PixelForge quality gate on the owner machine.
' Double-click, or: cscript //nologo RUN_QUALITY_GATE.vbs
' This does not publish a release. It writes _benchmarks\quality-gate\GATE_RESULTS.json.
' quality-gate.done is written only when the shipping Vulkan executable actually runs
' and the still-image visibility checks pass. A CPU-only run writes quality-gate.cpu.done.

Option Explicit

Dim shell, fso, root, command, code, interactive
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
shell.CurrentDirectory = root
interactive = (InStr(1, LCase(WScript.FullName), "cscript", vbTextCompare) = 0)

command = "cmd /c python tools\quality_gate.py --full & echo. & echo GPU still required when GATE_RESULTS.json lists owner_must_run. & echo NVIDIA RTX VSR and libvmaf are not part of the CPU score."
code = shell.Run(command, 1, True)

If interactive Then
  If code = 0 Then
    MsgBox "Quality gate finished." & vbCrLf & "See _benchmarks\quality-gate\GATE_RESULTS.json", 64, "PixelForge quality gate"
  Else
    MsgBox "Quality gate exited with code " & code & "." & vbCrLf & _
      "0 = requested scope passed" & vbCrLf & _
      "2 = a model ran but was not visibly different from Lanczos" & vbCrLf & _
      "3 = Real-ESRGAN or the Vulkan executable did not run" & vbCrLf & vbCrLf & _
      "Read _benchmarks\quality-gate\GATE_RESULTS.json before changing any public claim.", 48, "PixelForge quality gate"
  End If
Else
  WScript.Echo "quality gate exit " & code
End If

WScript.Quit code
