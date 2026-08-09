param(
  [string]$Version = "1.0.22"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ($Version -notmatch '^\d+\.\d+\.\d+$') {
  throw "Version must use semantic form such as 1.0.22"
}

if (!(Test-Path ".venv")) {
  python -m venv .venv
}

& "$root\.venv\Scripts\python.exe" -m pip install --upgrade pip
& "$root\.venv\Scripts\python.exe" -m pip install -r requirements.txt

$releaseRoot = Join-Path $root "release"
$releaseDir = Join-Path $releaseRoot "v$Version"
$buildRoot = Join-Path $root "build"
$buildDir = Join-Path $buildRoot "v$Version"
if (Test-Path $releaseDir) { Remove-Item -LiteralPath $releaseDir -Recurse -Force }
if (Test-Path $buildDir) { Remove-Item -LiteralPath $buildDir -Recurse -Force }
New-Item -ItemType Directory -Path $releaseDir | Out-Null
New-Item -ItemType Directory -Path $buildDir | Out-Null

$iconArg = @()
$iconPath = Join-Path $root "assets\icons\pixelforge_app.ico"
if (Test-Path $iconPath) {
  $iconArg = @("--icon", $iconPath)
}

# PyInstaller's maintained hooks collect the NumPy and ONNX Runtime DLLs. Explicit
# collect-all flags also bundled their test suites and developer-only tooling.
& "$root\.venv\Scripts\python.exe" -m PyInstaller --noconfirm --clean --onefile --windowed `
  --name "PixelForge-AI" `
  --distpath (Join-Path $buildDir "dist") `
  --workpath (Join-Path $buildDir "work") `
  --specpath $buildDir `
  --collect-data PIL `
  --collect-data stripe `
  --collect-data certifi `
  --collect-all webview `
  --exclude-module onnxruntime.quantization `
  --exclude-module onnxruntime.tools `
  --exclude-module onnxruntime.transformers `
  --exclude-module numpy.f2py `
  --exclude-module numpy.testing `
  --exclude-module numpy.tests `
  --version-file (Join-Path $root "assets\version_info.txt") `
  --add-binary "$(Join-Path $root 'realesrgan-ncnn-vulkan.exe');." `
  --add-binary "$(Join-Path $root 'realsr-ncnn-vulkan.exe');." `
  --add-binary "$(Join-Path $root 'waifu2x-ncnn-vulkan.exe');." `
  --add-binary "$(Join-Path $root 'rife-ncnn-vulkan.exe');." `
  --add-binary "$(Join-Path $root 'realcugan-ncnn-vulkan.exe');." `
  --add-binary "$(Join-Path $root 'srmd-ncnn-vulkan.exe');." `
  --add-data "$(Join-Path $root 'assets');assets" `
  --add-data "$(Join-Path $root 'models');models" `
  --add-data "$(Join-Path $root 'realsr-models');realsr-models" `
  --add-data "$(Join-Path $root 'waifu2x-models');waifu2x-models" `
  --add-data "$(Join-Path $root 'rife-models');rife-models" `
  --add-data "$(Join-Path $root 'realcugan-models');realcugan-models" `
  --add-data "$(Join-Path $root 'models-srmd');models-srmd" `
  --add-data "$(Join-Path $root 'onnx-models');onnx-models" `
  @iconArg `
  (Join-Path $root "process_full_video_ultimate.py")

if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller failed with exit code $LASTEXITCODE"
}


Copy-Item (Join-Path $buildDir "dist\PixelForge-AI.exe") (Join-Path $releaseDir "PixelForge-AI.exe") -Force
Copy-Item (Join-Path $root "README.md") (Join-Path $releaseDir "README.md") -Force
Copy-Item (Join-Path $root "onnx-models\MODEL_NOTICES.md") (Join-Path $releaseDir "MODEL_NOTICES.md") -Force
Copy-Item (Join-Path $root "RELEASE_NOTES_v$Version.md") (Join-Path $releaseDir "RELEASE_NOTES.md") -Force

$zipName = "PixelForge-AI_${Version}_windows_x64.zip"
$zipPath = Join-Path $releaseDir $zipName
Compress-Archive -Path @(
  (Join-Path $releaseDir "PixelForge-AI.exe"),
  (Join-Path $releaseDir "README.md"),
  (Join-Path $releaseDir "MODEL_NOTICES.md"),
  (Join-Path $releaseDir "RELEASE_NOTES.md")
) -DestinationPath $zipPath

$hashLines = @()
foreach ($artifact in @((Join-Path $releaseDir "PixelForge-AI.exe"), $zipPath)) {
  $hash = Get-FileHash -Algorithm SHA256 -LiteralPath $artifact
  $hashLines += "$($hash.Hash.ToLowerInvariant())  $([System.IO.Path]::GetFileName($artifact))"
}
Set-Content -LiteralPath (Join-Path $releaseDir "SHA256SUMS.txt") -Value $hashLines -Encoding ascii

Write-Host "Release build complete: $releaseDir"
