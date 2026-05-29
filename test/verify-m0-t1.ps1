$ErrorActionPreference = 'Stop'

$requiredPaths = @(
  'go.mod',
  'Makefile',
  'Dockerfile',
  '.github/workflows/ci.yml',
  'cmd/gateway/main.go',
  'internal/config/.gitkeep',
  'internal/listener/.gitkeep',
  'internal/codec/.gitkeep',
  'internal/session/.gitkeep',
  'internal/auth/.gitkeep',
  'internal/limiter/.gitkeep',
  'internal/context/.gitkeep',
  'internal/dify/client.go',
  'internal/dify/sse.go',
  'internal/dify/types.go',
  'internal/moderation/.gitkeep',
  'internal/mux/.gitkeep',
  'internal/store/.gitkeep',
  'internal/telemetry/.gitkeep',
  'api/proto/gateway.proto',
  'deploy/.gitkeep',
  'test/.gitkeep'
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
