$ErrorActionPreference = 'Stop'

# Verify the M0-T1 skeleton directories exist. These started as .gitkeep
# placeholders; later milestones replace the placeholders with real code, so we
# assert the directory exists rather than a specific placeholder file.
$requiredPaths = @(
  'go.mod',
  'Makefile',
  'Dockerfile',
  '.github/workflows/ci.yml',
  'cmd/gateway/main.go',
  'internal/config',
  'internal/listener',
  'internal/codec',
  'internal/session',
  'internal/auth',
  'internal/limiter',
  'internal/context',
  'internal/dify/client.go',
  'internal/dify/sse.go',
  'internal/dify/types.go',
  'internal/moderation',
  'internal/mux',
  'internal/store',
  'internal/telemetry',
  'api/proto/gateway.proto',
  'deploy',
  'test'
)

$missing = @()
foreach ($path in $requiredPaths) {
  if (-not (Test-Path -LiteralPath $path)) {
    $missing += $path
  }
}

if ($missing.Count -gt 0) {
  Write-Error ("Missing M0-T1 skeleton paths: " + ($missing -join ', '))
}

$goMod = Get-Content -Raw -Path 'go.mod' -ErrorAction Stop
if ($goMod -notmatch 'module\s+github\.com/.+/game-gateway|module\s+dify_gateway') {
  Write-Error 'go.mod must declare a stable module path'
}

$makefile = Get-Content -Raw -Path 'Makefile' -ErrorAction Stop
foreach ($target in @('build:', 'test:', 'lint:')) {
  if ($makefile -notmatch [regex]::Escape($target)) {
    Write-Error "Makefile missing target $target"
  }
}

Write-Host 'M0-T1 skeleton paths verified.'
