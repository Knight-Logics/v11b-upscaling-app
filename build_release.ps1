param(
  [string]$Version = "1.0.16"
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
  --collect-data stripe `
  --collect-data certifi `
  --collect-all webview `
  --add-binary "realesrgan-ncnn-vulkan.exe;." `
  --add-binary "realsr-ncnn-vulkan.exe;." `
  --add-binary "waifu2x-ncnn-vulkan.exe;." `
  --add-binary "rife-ncnn-vulkan.exe;." `
  --add-data "assets;assets" `
  --add-data "models;models" `
  --add-data "realsr-models;realsr-models" `
  --add-data "waifu2x-models;waifu2x-models" `
  --add-data "rife-models;rife-models" `
  @iconArg `
  process_full_video_ultimate.py

Copy-Item (Join-Path $root "dist\PixelForge-AI.exe") (Join-Path $releaseDir "PixelForge-AI.exe") -Force

# Bundle runtime model/tool files needed by app pipeline
$runtimeExes = @(
  "realesrgan-ncnn-vulkan.exe",
  "realsr-ncnn-vulkan.exe",
  "waifu2x-ncnn-vulkan.exe",
  "rife-ncnn-vulkan.exe"
)
foreach ($exe in $runtimeExes) {
  if (Test-Path (Join-Path $root $exe)) {
    Copy-Item (Join-Path $root $exe) (Join-Path $releaseDir $exe) -Force
  }
}

$runtimeDirs = @("models", "realsr-models", "waifu2x-models", "rife-models")
foreach ($dir in $runtimeDirs) {
  if (Test-Path (Join-Path $root $dir)) {
    Copy-Item (Join-Path $root $dir) (Join-Path $releaseDir $dir) -Recurse -Force
  }
}

foreach ($dll in @("vcomp140.dll", "vcomp140d.dll")) {
  if (Test-Path (Join-Path $root $dll)) {
    Copy-Item (Join-Path $root $dll) (Join-Path $releaseDir $dll) -Force
  }
}

$zipName = "PixelForge-AI_${Version}_windows_x64.zip"
$zipPath = Join-Path $releaseDir $zipName
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $releaseDir "*") -DestinationPath $zipPath

Write-Host "Release build complete: $releaseDir"
