param(
  [string]$Version = "1.0.22"
)

# Microsoft Store (MSIX) build of PixelForge AI.
# Differences from build_release.ps1:
#   * --onedir instead of --onefile (no 390 MB self-extraction on every launch;
#     MSIX installs to a read-only folder, so nothing needs to unpack at runtime)
#   * pixelforge/distribution.py STORE_BUILD flipped to True inside the build copy
#     only (the tracked file is restored afterwards)
#   * output goes to store\v<Version>\layout, ready for msix\AppxManifest.xml

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ($Version -notmatch '^\d+\.\d+\.\d+$') { throw "Version must look like 1.0.22" }

if (!(Test-Path ".venv")) { python -m venv .venv }
& "$root\.venv\Scripts\python.exe" -m pip install -r requirements.txt

$storeRoot = Join-Path $root "store\v$Version"
$buildDir  = Join-Path $root "build\store-v$Version"
foreach ($d in @($storeRoot, $buildDir)) { if (Test-Path $d) { Remove-Item -LiteralPath $d -Recurse -Force } }
New-Item -ItemType Directory -Path $storeRoot, $buildDir | Out-Null

$distFile = Join-Path $root "pixelforge\distribution.py"
$original = Get-Content -LiteralPath $distFile -Raw
try {
  $utf8 = New-Object System.Text.UTF8Encoding($false)   # no BOM
  [IO.File]::WriteAllText($distFile, ($original -replace '(?m)^STORE_BUILD = False', 'STORE_BUILD = True'), $utf8)
  if (-not (Select-String -LiteralPath $distFile -Pattern '^STORE_BUILD = True' -Quiet)) {
    throw "Could not flip STORE_BUILD"
  }

  & "$root\.venv\Scripts\python.exe" -m PyInstaller --noconfirm --clean --onedir --windowed `
    --name "PixelForge-AI" `
    --distpath (Join-Path $buildDir "dist") `
    --workpath (Join-Path $buildDir "work") `
    --specpath $buildDir `
    --collect-data PIL --collect-data stripe --collect-data certifi --collect-all webview `
    --exclude-module onnxruntime.quantization --exclude-module onnxruntime.tools `
    --exclude-module onnxruntime.transformers --exclude-module numpy.f2py `
    --exclude-module numpy.testing --exclude-module numpy.tests `
    --version-file (Join-Path $root "assets\version_info.txt") `
    --icon (Join-Path $root "assets\icons\pixelforge_app.ico") `
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
    (Join-Path $root "process_full_video_ultimate.py")
  if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed with exit code $LASTEXITCODE" }
}
finally {
  [IO.File]::WriteAllText($distFile, $original, (New-Object System.Text.UTF8Encoding($false)))
}

# Store layout (updater.py is present but inert: IS_STORE_BUILD short-circuits it).
$layout = Join-Path $storeRoot "layout"
Copy-Item (Join-Path $buildDir "dist\PixelForge-AI") $layout -Recurse
Copy-Item (Join-Path $root "onnx-models\MODEL_NOTICES.md") (Join-Path $layout "MODEL_NOTICES.md") -Force

# MSIX manifest + tile images (msix\ folder is added by this change set)
Copy-Item (Join-Path $root "msix\AppxManifest.xml") (Join-Path $layout "AppxManifest.xml") -Force
Copy-Item (Join-Path $root "msix\Images") (Join-Path $layout "Images") -Recurse -Force
(Get-Content (Join-Path $layout "AppxManifest.xml") -Raw) -replace 'Version="[0-9.]+"\s+/>', "Version=`"$Version.0`" />" |
  Set-Content (Join-Path $layout "AppxManifest.xml") -Encoding utf8

Write-Host "Store layout ready: $layout"
Write-Host "Next: makeappx pack /d `"$layout`" /p `"$storeRoot\PixelForgeAI_$Version.0_x64.msix`" /o"
