# LANCE Docker Integration Test Runner (Windows)
# Builds and runs a 3-node cluster in Docker, runs tests, then tears down
#
# Usage:
#   .\scripts\run-docker-tests.ps1                     # Run all integration tests
#   .\scripts\run-docker-tests.ps1 -Filter "multi"     # Run tests matching filter
#   .\scripts\run-docker-tests.ps1 -SkipCluster        # Skip cluster tests
#   .\scripts\run-docker-tests.ps1 -NoBuild            # Skip Docker build
#   .\scripts\run-docker-tests.ps1 -Keep               # Keep containers running after tests

param(
    [string]$Filter = "",
    [switch]$SkipCluster,
    [switch]$NoBuild,
    [switch]$Keep,
    [switch]$Verbose,
    [string]$Target = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir
$ComposeFile = Join-Path $ProjectDir "docker-compose.test.yml"

$script:TestExitCode = 0
$StartTime = Get-Date

function Write-Status {
    param([string]$Message, [string]$Color = "Cyan")
    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "[$timestamp] $Message" -ForegroundColor $Color
}

function Test-DockerAvailable {
    Write-Status "Checking Docker availability..." "Yellow"
    
    try {
        $null = docker version 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Docker is not running"
        }
    } catch {
        Write-Status "Docker is not installed or not running. Please start Docker Desktop." "Red"
        exit 1
    }
    
    # Check for docker-compose or docker compose
    $script:ComposeCmd = $null
    try {
        $null = docker-compose version 2>&1
        if ($LASTEXITCODE -eq 0) {
            $script:ComposeCmd = "docker-compose"
        }
    } catch {}
    
    if (-not $script:ComposeCmd) {
        try {
            $null = docker compose version 2>&1
            if ($LASTEXITCODE -eq 0) {
                $script:ComposeCmd = "docker compose"
            }
        } catch {}
    }
    
    if (-not $script:ComposeCmd) {
        Write-Status "docker-compose is not available" "Red"
        exit 1
    }
    
    Write-Status "Docker is available (using: $($script:ComposeCmd))" "Green"
}

function Invoke-Compose {
    param([string[]]$Arguments)
    
    if ($script:ComposeCmd -eq "docker compose") {
        & docker compose -f $ComposeFile @Arguments
    } else {
        & docker-compose -f $ComposeFile @Arguments
    }
}

function Show-ContainerLogs {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Container Logs" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    foreach ($node in @("lance-node0", "lance-node1", "lance-node2")) {
        Write-Host ""
        Write-Host "--- $node ---" -ForegroundColor Yellow
        try {
            $logs = docker logs $node --tail 50 2>&1
            $logs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        } catch {
            Write-Host "  (no logs available)" -ForegroundColor DarkGray
        }
    }
    Write-Host ""
}

function Stop-Cluster {
    if ($Keep) {
        Write-Status "Keeping containers running (use 'docker-compose -f docker-compose.test.yml down' to stop)" "Yellow"
        return
    }
    
    Write-Status "Stopping Docker containers..." "Yellow"
    
    Push-Location $ProjectDir
    try {
        # Use Start-Process to avoid hanging on stderr
        $process = Start-Process -FilePath "docker" -ArgumentList "compose", "-f", $ComposeFile, "down", "-v" -Wait -PassThru -NoNewWindow
    } catch {
        # Ignore errors during cleanup
    } finally {
        Pop-Location
    }
    
    Write-Status "Containers stopped." "Green"
}

function Build-Images {
    if ($NoBuild) {
        Write-Status "Skipping Docker build..." "Yellow"
        return
    }
    
    Write-Status "Building Docker images (this may take a few minutes)..." "Yellow"
    
    Push-Location $ProjectDir
    try {
        if ($Verbose) {
            Invoke-Compose @("build")
        } else {
            Invoke-Compose @("build", "--quiet")
        }
        
        if ($LASTEXITCODE -ne 0) {
            throw "Docker build failed"
        }
    } finally {
        Pop-Location
    }
    
    Write-Status "Docker images built." "Green"
}

function Start-Cluster {
    Write-Status "Starting 3-node LANCE cluster..." "Cyan"
    
    Push-Location $ProjectDir
    try {
        # Stop any existing containers
        Invoke-Compose @("down", "-v") 2>$null
        
        # Start containers
        Invoke-Compose @("up", "-d")
        
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to start containers"
        }
    } finally {
        Pop-Location
    }
    
    # Wait for containers to be healthy
    Write-Status "Waiting for containers to be healthy..." "Yellow"
    $maxWait = 60
    $ready = $false
    
    for ($wait = 0; $wait -lt $maxWait; $wait++) {
        $healthy = 0
        
        foreach ($node in @("lance-node0", "lance-node1", "lance-node2")) {
            try {
                $status = docker inspect --format='{{.State.Health.Status}}' $node 2>$null
                if ($status -eq "healthy") {
                    $healthy++
                }
            } catch {
                # Container might not exist yet
            }
        }
        
        if ($healthy -eq 3) {
            $ready = $true
            break
        }
        
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 1
    }
    Write-Host ""
    
    if (-not $ready) {
        Write-Status "Containers failed to become healthy. Checking logs..." "Red"
        
        foreach ($node in @("lance-node0", "lance-node1", "lance-node2")) {
            Write-Status "=== $node logs ===" "Red"
            docker logs $node --tail 20 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        }
        
        throw "Cluster failed to start"
    }
    
    Write-Status "Cluster is ready!" "Green"
    Write-Status "  Node 0: localhost:1992" "Gray"
    Write-Status "  Node 1: localhost:1993" "Gray"
    Write-Status "  Node 2: localhost:1994" "Gray"
}

function Set-TestEnvironment {
    Write-Status "Setting environment variables..." "Cyan"
    
    $env:LANCE_TEST_ADDR = "127.0.0.1:1992"
    $env:LANCE_NODE1_ADDR = "127.0.0.1:1992"
    $env:LANCE_NODE2_ADDR = "127.0.0.1:1993"
    $env:LANCE_NODE3_ADDR = "127.0.0.1:1994"
    
    Write-Status "  LANCE_TEST_ADDR = $env:LANCE_TEST_ADDR" "Gray"
    Write-Status "  LANCE_NODE1_ADDR = $env:LANCE_NODE1_ADDR" "Gray"
    Write-Status "  LANCE_NODE2_ADDR = $env:LANCE_NODE2_ADDR" "Gray"
    Write-Status "  LANCE_NODE3_ADDR = $env:LANCE_NODE3_ADDR" "Gray"
}

function Invoke-Tests {
    Write-Status "Running integration tests..." "Cyan"
    
    $testArgs = @(
        "test",
        "--package", "lnc-client",
        "--test", "integration",
        "--",
        "--ignored",
        "--nocapture"
    )
    
    # Add test filter if specified
    if ($Filter) {
        $testArgs += $Filter
        Write-Status "  Filter: $Filter" "Gray"
    }
    
    # Exclude cluster tests if requested
    if ($SkipCluster) {
        Write-Status "  Skipping cluster tests" "Gray"
        $testArgs += "--skip"
        $testArgs += "cluster"
    }
    
    Write-Status "  Command: cargo $($testArgs -join ' ')" "Gray"
    Write-Host ""
    
    Push-Location $ProjectDir
    try {
        & cargo @testArgs
        $script:TestExitCode = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    
    Write-Host ""
    if ($script:TestExitCode -eq 0) {
        Write-Status "All tests passed!" "Green"
    } else {
        Write-Status "Some tests failed (exit code: $script:TestExitCode)" "Red"
    }
}

function Test-TargetEndpoint {
    param([string]$Endpoint)
    
    $parts = $Endpoint -split ':'
    $host_ = $parts[0]
    $port = [int]$parts[1]
    
    Write-Status "Verifying target endpoint: $Endpoint" "Cyan"
    
    # Default ports
    $metricsPort = 9090
    $healthPort = 8080
    
    # If not default port, try to calculate
    if ($port -ne 1992) {
        $metricsPort = $port + 7098
        $healthPort = $port + 6088
    }
    
    Write-Host ""
    Write-Status "=== Endpoint Connectivity ===" "Yellow"
    
    # Test client port
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect($host_, $port)
        $tcp.Close()
        Write-Status "  ✓ Client port (${host_}:${port}): reachable" "Green"
    } catch {
        Write-Status "  ✗ Client port (${host_}:${port}): not reachable" "Red"
        $script:TestExitCode = 1
        return $false
    }
    
    Write-Host ""
    Write-Status "=== Health Check ===" "Yellow"
    
    # Try health endpoint
    try {
        $healthResponse = Invoke-RestMethod -Uri "http://${host_}:${healthPort}/health/ready" -TimeoutSec 5 -ErrorAction SilentlyContinue
        if ($healthResponse.status -eq "ready") {
            Write-Status "  ✓ Health endpoint (${host_}:${healthPort}): ready" "Green"
            Write-Status "    Response: $($healthResponse | ConvertTo-Json -Compress)" "Gray"
        } else {
            Write-Status "  ⚠ Health endpoint (${host_}:${healthPort}): responded but not ready" "Yellow"
        }
    } catch {
        Write-Status "  ⚠ Health endpoint (${host_}:${healthPort}): not reachable (may be on different port)" "Yellow"
    }
    
    Write-Host ""
    Write-Status "=== Metrics Verification ===" "Yellow"
    
    # Try metrics endpoint
    try {
        $metricsResponse = Invoke-WebRequest -Uri "http://${host_}:${metricsPort}/metrics" -TimeoutSec 5 -ErrorAction Stop
        $metricsContent = $metricsResponse.Content
        $metricsLength = $metricsContent.Length
        
        if ($metricsLength -gt 100) {
            $lanceMetrics = ($metricsContent -split "`n" | Where-Object { $_ -match "^lance_" })
            $metricCount = $lanceMetrics.Count
            
            if ($metricCount -gt 0) {
                Write-Status "  ✓ Metrics endpoint (${host_}:${metricsPort}): OK" "Green"
                Write-Status "    Response size: $metricsLength bytes" "Gray"
                Write-Status "    LANCE metrics found: $metricCount" "Gray"
                Write-Host ""
                Write-Status "  Sample metrics:" "Cyan"
                $lanceMetrics | Select-Object -First 10 | ForEach-Object {
                    Write-Status "    $_" "Gray"
                }
            } else {
                Write-Status "  ⚠ Metrics endpoint (${host_}:${metricsPort}): responding but no lance_* metrics" "Yellow"
                Write-Status "    Response size: $metricsLength bytes" "Gray"
                Write-Status "    This may indicate metrics are not being exported properly." "Yellow"
                $script:TestExitCode = 1
            }
        } else {
            Write-Status "  ⚠ Metrics endpoint (${host_}:${metricsPort}): response too short ($metricsLength bytes)" "Yellow"
            $script:TestExitCode = 1
        }
    } catch {
        Write-Status "  ⚠ Metrics endpoint (${host_}:${metricsPort}): not reachable (may be on different port)" "Yellow"
    }
    
    return $true
}

function Invoke-TargetTests {
    param([string]$Endpoint)
    
    Write-Status "Running tests against target endpoint: $Endpoint" "Cyan"
    Write-Host ""
    
    # Verify endpoint first
    if (-not (Test-TargetEndpoint -Endpoint $Endpoint)) {
        return
    }
    
    Write-Host ""
    Write-Status "=== Integration Tests ===" "Yellow"
    
    # Set environment for tests
    $env:LANCE_TEST_ADDR = $Endpoint
    $env:LANCE_NODE1_ADDR = $Endpoint
    $env:LANCE_NODE2_ADDR = $Endpoint
    $env:LANCE_NODE3_ADDR = $Endpoint
    
    Write-Status "  LANCE_TEST_ADDR = $env:LANCE_TEST_ADDR" "Gray"
    Write-Host ""
    
    $testArgs = @(
        "test",
        "--package", "lnc-client",
        "--test", "integration",
        "--",
        "--ignored",
        "--nocapture",
        "--skip", "cluster"
    )
    
    if ($Filter) {
        $testArgs += $Filter
        Write-Status "  Filter: $Filter" "Gray"
    }
    
    Write-Status "  Command: cargo $($testArgs -join ' ')" "Gray"
    Write-Host ""
    
    Push-Location $ProjectDir
    try {
        & cargo @testArgs
        $script:TestExitCode = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    
    Write-Host ""
    if ($script:TestExitCode -eq 0) {
        Write-Status "✓ All target tests passed!" "Green"
    } else {
        Write-Status "Some tests failed (exit code: $script:TestExitCode)" "Red"
    }
}

# Main
try {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  LANCE Docker Integration Test Runner" -ForegroundColor Cyan
    Write-Host "  (Windows)" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Target mode: run tests against a running endpoint
    if ($Target) {
        Write-Status "Configuration:" "Cyan"
        Write-Status "  Target endpoint: $Target" "Gray"
        if ($Filter) {
            Write-Status "  Filter: $Filter" "Gray"
        }
        Write-Host ""
        
        Invoke-TargetTests -Endpoint $Target
        
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  Target Test Summary" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        if ($script:TestExitCode -eq 0) {
            Write-Status "✓ All target tests completed successfully!" "Green"
        } else {
            Write-Status "Some tests failed (exit code: $script:TestExitCode)" "Red"
        }
        
        exit $script:TestExitCode
    }
    
    # Docker mode
    Test-DockerAvailable
    Build-Images
    Start-Cluster
    Set-TestEnvironment
    Invoke-Tests
    
} catch {
    Write-Status "Error: $_" "Red"
    $script:TestExitCode = 1
} finally {
    # Show container logs before cleanup
    Show-ContainerLogs
    
    Stop-Cluster
    
    $elapsed = (Get-Date) - $StartTime
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Status "Total time: $([Math]::Round($elapsed.TotalSeconds, 1)) seconds" "Cyan"
    Write-Host "========================================" -ForegroundColor Cyan
}

exit $script:TestExitCode
