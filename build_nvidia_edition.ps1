param(
  [string]$AppVersion = "1.0.22"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ($AppVersion -notmatch '^\d+\.\d+\.\d+$') {
  throw "AppVersion must use semantic form such as 1.0.22"
}

$appReleaseDir = Join-Path $root "release\v$AppVersion"
$appExe = Join-Path $appReleaseDir "PixelForge-AI.exe"
$nvidiaPackDir = Join-Path $root "release\nvidia-pack-v$AppVersion"
if (!(Test-Path -LiteralPath $appExe)) {
  throw "Build the universal app first: $appExe"
}
if (!(Test-Path -LiteralPath (Join-Path $nvidiaPackDir "PixelForge-NVIDIA-Worker.exe"))) {
  throw "Build the NVIDIA pack first: $nvidiaPackDir"
}

$bundleRoot = Join-Path $root "build\nvidia-edition-v$AppVersion"
if (Test-Path -LiteralPath $bundleRoot) { Remove-Item -LiteralPath $bundleRoot -Recurse -Force }
New-Item -ItemType Directory -Path $bundleRoot | Out-Null
Copy-Item -LiteralPath $appExe -Destination $bundleRoot -Force
foreach ($document in @("README.md", "MODEL_NOTICES.md", "RELEASE_NOTES.md")) {
  $documentPath = Join-Path $appReleaseDir $document
  if (Test-Path -LiteralPath $documentPath) {
    Copy-Item -LiteralPath $documentPath -Destination $bundleRoot -Force
  }
}

$sidecarRoot = Join-Path $bundleRoot "nvidia-pack"
New-Item -ItemType Directory -Path $sidecarRoot | Out-Null
Copy-Item -Path (Join-Path $nvidiaPackDir "*") -Destination $sidecarRoot -Recurse -Force
Set-Content -LiteralPath (Join-Path $bundleRoot "README-NVIDIA-EDITION.txt") -Value @"
PixelForge AI NVIDIA RTX Edition $AppVersion

Extract the complete folder and run PixelForge-AI.exe. PixelForge detects the
bundled NVIDIA engine automatically on compatible RTX/Tensor Core hardware.
Balanced remains the best overall default. Choose NVIDIA RTX for the fastest
compatible SDR path, or Max Detail for the highest-fidelity / slowest path.
"@ -Encoding utf8

$nvidiaEditionZip = Join-Path $appReleaseDir "PixelForge-AI-NVIDIA_${AppVersion}_windows_x64.zip"
if (Test-Path -LiteralPath $nvidiaEditionZip) { Remove-Item -LiteralPath $nvidiaEditionZip -Force }
Compress-Archive -Path (Join-Path $bundleRoot "*") -DestinationPath $nvidiaEditionZip -CompressionLevel Optimal
Write-Host "All-in-one NVIDIA edition complete: $nvidiaEditionZip"
