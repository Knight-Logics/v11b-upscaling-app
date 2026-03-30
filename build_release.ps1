param(
  [string]$Version = "1.0.3"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (!(Test-Path ".venv")) {
  python -m venv .venv
}

& "$root\.venv\Scripts\python.exe" -m pip install --upgrade pip
& "$root\.venv\Scripts\python.exe" -m pip install -r requirements.txt

$releaseDir = Join-Path $root "release"
$buildDir = Join-Path $root "build"
if (Test-Path $releaseDir) { Remove-Item $releaseDir -Recurse -Force }
if (Test-Path $buildDir) { Remove-Item $buildDir -Recurse -Force }
New-Item -ItemType Directory -Path $releaseDir | Out-Null

$iconArg = @()
$iconPath = Join-Path $root "assets\icons\pixelforge_app.ico"
if (Test-Path $iconPath) {
  $iconArg = @("--icon", $iconPath)
}

& "$root\.venv\Scripts\pyinstaller.exe" --noconfirm --clean --onefile --windowed `
  --name "PixelForge-AI" `
  --collect-data PIL `
  --add-data "assets;assets" `
  --add-data "models;models" `
  @iconArg `
  process_full_video_ultimate.py

Copy-Item (Join-Path $root "dist\PixelForge-AI.exe") (Join-Path $releaseDir "PixelForge-AI.exe") -Force

# Bundle runtime model/tool files needed by app pipeline
if (Test-Path (Join-Path $root "realesrgan-ncnn-vulkan.exe")) {
  Copy-Item (Join-Path $root "realesrgan-ncnn-vulkan.exe") (Join-Path $releaseDir "realesrgan-ncnn-vulkan.exe") -Force
}
if (Test-Path (Join-Path $root "models")) {
  Copy-Item (Join-Path $root "models") (Join-Path $releaseDir "models") -Recurse -Force
}
if (Test-Path (Join-Path $root "vcomp140.dll")) {
  Copy-Item (Join-Path $root "vcomp140.dll") (Join-Path $releaseDir "vcomp140.dll") -Force
}
if (Test-Path (Join-Path $root "vcomp140d.dll")) {
  Copy-Item (Join-Path $root "vcomp140d.dll") (Join-Path $releaseDir "vcomp140d.dll") -Force
}

$zipName = "PixelForge-AI_${Version}_windows_x64.zip"
$zipPath = Join-Path $releaseDir $zipName
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $releaseDir "*") -DestinationPath $zipPath

Write-Host "Release build complete: $releaseDir"
