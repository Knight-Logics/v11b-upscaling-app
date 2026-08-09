param(
  [string]$AppVersion = "1.0.22",
  [string]$PackPath = "",
  [string]$ModulePath = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if ($AppVersion -notmatch '^\d+\.\d+\.\d+$') {
  throw "AppVersion must use semantic form such as 1.0.22"
}
if ([string]::IsNullOrWhiteSpace($PackPath)) {
  $PackPath = Join-Path $root "release\PixelForge-NVIDIA-Pack_${AppVersion}_windows_x64.zip"
}
if ([string]::IsNullOrWhiteSpace($ModulePath)) {
  $ModulePath = Join-Path $root "pixelforge\nvidia.py"
}
if (!(Test-Path -LiteralPath $PackPath)) { throw "NVIDIA pack not found: $PackPath" }
if (!(Test-Path -LiteralPath $ModulePath)) { throw "NVIDIA module not found: $ModulePath" }

$hash = (Get-FileHash -LiteralPath $PackPath -Algorithm SHA256).Hash.ToLowerInvariant()
$urlFragment = "v$AppVersion/PixelForge-NVIDIA-Pack_${AppVersion}_windows_x64.zip"
$content = Get-Content -Raw -LiteralPath $ModulePath
$urlPattern = 'v\d+\.\d+\.\d+/PixelForge-NVIDIA-Pack_\d+\.\d+\.\d+_windows_x64\.zip'
$hashPattern = '(NVIDIA_PACK_SHA256\s*=\s*")[0-9a-f]{64}("\s*)'
if ([regex]::Matches($content, $urlPattern).Count -ne 1) { throw "Expected one NVIDIA pack URL" }
if ([regex]::Matches($content, $hashPattern).Count -ne 1) { throw "Expected one NVIDIA pack hash" }
$updated = [regex]::Replace(
  $content,
  $urlPattern,
  $urlFragment,
  1
)
$updated = [regex]::Replace(
  $updated,
  $hashPattern,
  "`$1$hash`$2",
  1
)
if ($updated -notmatch [regex]::Escape($urlFragment) -or $updated -notmatch [regex]::Escape($hash)) {
  throw "Could not stamp NVIDIA release URL and hash"
}
[IO.File]::WriteAllText($ModulePath, $updated, [Text.UTF8Encoding]::new($false))
Write-Host "Pinned NVIDIA pack for v${AppVersion}: $hash"
