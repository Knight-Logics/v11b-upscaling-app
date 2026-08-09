param(
  [string]$AppVersion = "1.0.20"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ($AppVersion -notmatch '^\d+\.\d+\.\d+$') {
  throw "AppVersion must use semantic form such as 1.0.20"
}

$venv = Join-Path $root ".venv-nvidia"
$python = Join-Path $venv "Scripts\python.exe"
if (!(Test-Path -LiteralPath $python)) {
  python -m venv $venv
}
& $python -m pip install --upgrade pip
& $python -m pip install -r (Join-Path $root "requirements-nvidia.txt")

$buildRoot = Join-Path $root "build\nvidia-pack-v$AppVersion"
$releaseRoot = Join-Path $root "release\nvidia-pack-v$AppVersion"
$distRoot = Join-Path $buildRoot "dist"
if (Test-Path -LiteralPath $buildRoot) { Remove-Item -LiteralPath $buildRoot -Recurse -Force }
if (Test-Path -LiteralPath $releaseRoot) { Remove-Item -LiteralPath $releaseRoot -Recurse -Force }
New-Item -ItemType Directory -Path $buildRoot | Out-Null
New-Item -ItemType Directory -Path $releaseRoot | Out-Null

& $python -m PyInstaller --noconfirm --clean --onedir --console `
  --name "PixelForge-NVIDIA-Worker" `
  --distpath $distRoot `
  --workpath (Join-Path $buildRoot "work") `
  --specpath $buildRoot `
  --collect-all nvvfx `
  --collect-all cupy `
  --collect-all cupy_backends `
  --collect-all cuda `
  --hidden-import graphlib `
  --exclude-module cupy.testing `
  --exclude-module cupyx.scipy `
  --exclude-module cupyx.distributed `
  --exclude-module numpy.testing `
  --exclude-module numpy.tests `
  (Join-Path $root "pixelforge\nvidia_worker.py")
if ($LASTEXITCODE -ne 0) {
  throw "NVIDIA worker PyInstaller build failed with exit code $LASTEXITCODE"
}

$builtDir = Join-Path $distRoot "PixelForge-NVIDIA-Worker"
Copy-Item -Path (Join-Path $builtDir "*") -Destination $releaseRoot -Recurse -Force

$licenseSource = Join-Path $venv "Lib\site-packages\nvidia_vfx-0.1.0.1.dist-info\licenses\packaging"
$licenseDestination = Join-Path $releaseRoot "licenses\NVIDIA-VFX"
New-Item -ItemType Directory -Path $licenseDestination -Force | Out-Null
Copy-Item -Path (Join-Path $licenseSource "*") -Destination $licenseDestination -Recurse -Force

$readme = @"
PixelForge AI NVIDIA RTX Engine Pack

Requires:
- PixelForge AI $AppVersion
- Windows 10/11 64-bit
- NVIDIA Tensor Core GPU (RTX/Turing or newer)
- NVIDIA Windows driver 570.65 or newer
- At least 4 GB VRAM (8 GB or more recommended)

Install through PixelForge AI's Enable NVIDIA RTX control, or extract this
archive into the app data nvidia-pack folder. The standard PixelForge profiles
do not require this pack.

NVIDIA VFX is currently used only for 8-bit SDR media. PixelForge keeps HDR and
10-bit sources on its FP32 DirectML precision path.
"@
Set-Content -LiteralPath (Join-Path $releaseRoot "README-NVIDIA.txt") -Value $readme -Encoding utf8

$zipName = "PixelForge-NVIDIA-Pack_${AppVersion}_windows_x64.zip"
$zipPath = Join-Path (Split-Path -Parent $releaseRoot) $zipName
if (Test-Path -LiteralPath $zipPath) { Remove-Item -LiteralPath $zipPath -Force }
Compress-Archive -Path (Join-Path $releaseRoot "*") -DestinationPath $zipPath -CompressionLevel Optimal
$hash = Get-FileHash -Algorithm SHA256 -LiteralPath $zipPath
Set-Content -LiteralPath "$zipPath.sha256" -Value "$($hash.Hash.ToLowerInvariant())  $zipName" -Encoding ascii

Write-Host "NVIDIA engine pack complete: $zipPath"
Write-Host "SHA256: $($hash.Hash.ToLowerInvariant())"
