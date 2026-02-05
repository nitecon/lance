# LANCE Integration Test Runner
# Runs integration tests in both single-node and 3-node cluster modes
#
# Usage:
#   .\scripts\run-integration-tests.ps1                    # Run all tests (both modes)
#   .\scripts\run-integration-tests.ps1 -Single            # Single-node tests only (L1)
#   .\scripts\run-integration-tests.ps1 -Cluster           # Cluster tests only (L3)
#   .\scripts\run-integration-tests.ps1 -TestFilter "multi" # Run tests matching filter
#   .\scripts\run-integration-tests.ps1 -SkipUnit          # Skip unit tests
#   .\scripts\run-integration-tests.ps1 -SkipCompliance    # Skip compliance checks
#   .\scripts\run-integration-tests.ps1 -Release           # Use release build

param(
    [string]$TestFilter = "",
    [switch]$Single,
    [switch]$Cluster,
    [switch]$SkipUnit,
    [switch]$SkipCompliance,
    [switch]$Release,
    [switch]$Verbose
)

$ErrorActionPreference = "Stop"
$script:Processes = @()

# Configuration
$BasePort = 1992
$BaseMetricsPort = 9090
$BaseHealthPort = 8080
$DataDirBase = "$PSScriptRoot\..\test-data"
$LogDir = "$PSScriptRoot\..\test-logs"
$ProjectDir = "$PSScriptRoot\.."

# Determine test mode
$TestMode = "both"
if ($Single -and -not $Cluster) { $TestMode = "single" }
if ($Cluster -and -not $Single) { $TestMode = "cluster" }

function Write-Status {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host "[$([DateTime]::Now.ToString('HH:mm:ss'))] $Message" -ForegroundColor $Color
}

function Stop-AllServers {
    Write-Status "Stopping all LANCE servers..." "Yellow"
    
    foreach ($proc in $script:Processes) {
        try {
            if (-not $proc.HasExited) {
                Write-Status "  Stopping server (PID: $($proc.Id))..." "Gray"
                $proc.Kill()
                $proc.WaitForExit(5000) | Out-Null
            }
        } catch {
            # Process already stopped
        }
    }
    
    # Kill any remaining lance processes on our ports
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $port = $BasePort + $i
        try {
            $connections = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
            foreach ($conn in $connections) {
                Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }
    
    $script:Processes = @()
    Write-Status "Servers stopped." "Green"
}

function Build-Lance {
    $buildType = if ($Release) { "release" } else { "debug" }
    $script:LanceBinary = "$ProjectDir\target\$buildType\lance.exe"
    
    if (-not (Test-Path $script:LanceBinary)) {
        Write-Status "Building LANCE ($buildType)..." "Yellow"
        Push-Location $ProjectDir
        try {
            if ($Release) {
                cargo build --release
            } else {
                cargo build
            }
            if ($LASTEXITCODE -ne 0) {
                throw "Build failed"
            }
        } finally {
            Pop-Location
        }
    }
    
    if (-not (Test-Path $script:LanceBinary)) {
        throw "LANCE binary not found at $script:LanceBinary"
    }
}

function Wait-ForServer {
    param([int]$Port, [int]$MaxWait = 10)
    
    for ($wait = 0; $wait -lt $MaxWait; $wait++) {
        Start-Sleep -Seconds 1
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect("127.0.0.1", $Port)
            $tcp.Close()
            return $true
        } catch {
            Write-Host "." -NoNewline
        }
    }
    Write-Host ""
    return $false
}

function Start-SingleNode {
    Write-Status "Starting LANCE single node (L1 mode)..." "Cyan"
    
    # Create directories
    if (-not (Test-Path $DataDirBase)) {
        New-Item -ItemType Directory -Path $DataDirBase -Force | Out-Null
    }
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    
    Build-Lance
    
    $dataDir = "$DataDirBase\single"
    $logFile = "$LogDir\single.log"
    $errFile = "$LogDir\single.err"
    
    # Clean and create data directory
    if (Test-Path $dataDir) {
        Remove-Item -Path $dataDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
    
    $args = @(
        "--node-id", "0",
        "--listen", "127.0.0.1:$BasePort",
        "--metrics", "127.0.0.1:$BaseMetricsPort",
        "--health", "127.0.0.1:$BaseHealthPort",
        "--data-dir", $dataDir,
        "--replication-mode", "l1"
    )
    
    Write-Status "  Starting single node on port $BasePort..." "White"
    
    $proc = Start-Process -FilePath $script:LanceBinary `
        -ArgumentList $args `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError $errFile `
        -PassThru `
        -WindowStyle Hidden
    
    $script:Processes += $proc
    
    if ($Verbose) {
        Write-Status "    PID: $($proc.Id), Log: $logFile" "Gray"
    }
    
    # Wait for server to be ready
    Write-Status "Waiting for server to initialize..." "Yellow"
    if (-not (Wait-ForServer -Port $BasePort -MaxWait 10)) {
        Write-Status "Server failed to start. Check logs in $LogDir" "Red"
        if (Test-Path $errFile) {
            $content = Get-Content $errFile -Tail 10 -ErrorAction SilentlyContinue
            if ($content) {
                $content | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
            }
        }
        throw "Single node failed to start"
    }
    
    Write-Status "Single node is ready at 127.0.0.1:$BasePort" "Green"
}

function Start-Cluster {
    $NodeCount = 3
    Write-Status "Starting LANCE cluster with $NodeCount nodes (L3 mode)..." "Cyan"
    
    # Create directories
    if (-not (Test-Path $DataDirBase)) {
        New-Item -ItemType Directory -Path $DataDirBase -Force | Out-Null
    }
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    
    Build-Lance
    
    # Build peer list for replication (using format: nodeId@host:replicationPort)
    # Port scheme per node: client=BasePort+(i*10), replication=client+1
    $peers = @()
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $nodeClientPort = $BasePort + ($i * 10)
        $nodeReplPort = $nodeClientPort + 1
        $peers += "${i}@127.0.0.1:${nodeReplPort}"
    }
    $peerList = $peers -join ","
    
    # Start each node
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $nodeId = $i
        $port = $BasePort + ($i * 10)          # Client: 1992, 2002, 2012
        $replicationPort = $port + 1            # Replication: 1993, 2003, 2013
        $metricsPort = $BaseMetricsPort + $i
        $healthPort = $BaseHealthPort + $i
        $dataDir = "$DataDirBase\node$nodeId"
        $logFile = "$LogDir\node$nodeId.log"
        $errFile = "$LogDir\node$nodeId.err"
        
        # Clean and create data directory
        if (Test-Path $dataDir) {
            Remove-Item -Path $dataDir -Recurse -Force
        }
        New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
        
        $args = @(
            "--node-id", $nodeId,
            "--listen", "127.0.0.1:$port",
            "--replication", "127.0.0.1:$replicationPort",
            "--metrics", "127.0.0.1:$metricsPort",
            "--health", "127.0.0.1:$healthPort",
            "--data-dir", $dataDir,
            "--replication-mode", "l3",
            "--peers", $peerList
        )
        
        Write-Status "  Starting node $nodeId on port $port..." "White"
        
        $proc = Start-Process -FilePath $script:LanceBinary `
            -ArgumentList $args `
            -RedirectStandardOutput $logFile `
            -RedirectStandardError $errFile `
            -PassThru `
            -WindowStyle Hidden
        
        $script:Processes += $proc
        
        if ($Verbose) {
            Write-Status "    PID: $($proc.Id), Log: $logFile" "Gray"
        }
    }
    
    # Wait for servers to be ready
    Write-Status "Waiting for servers to initialize..." "Yellow"
    $maxWait = 10
    $ready = $false
    
    for ($wait = 0; $wait -lt $maxWait; $wait++) {
        Start-Sleep -Seconds 1
        $allReady = $true
        
        for ($i = 0; $i -lt $NodeCount; $i++) {
            $port = $BasePort + ($i * 10)  # Match the spread port scheme
            try {
                $tcp = New-Object System.Net.Sockets.TcpClient
                $tcp.Connect("127.0.0.1", $port)
                $tcp.Close()
            } catch {
                $allReady = $false
                break
            }
        }
        
        if ($allReady) {
            $ready = $true
            break
        }
        
        Write-Host "." -NoNewline
    }
    Write-Host ""
    
    if (-not $ready) {
        Write-Status "Servers failed to start. Check logs in $LogDir" "Red"
        foreach ($i in 0..($NodeCount-1)) {
            $errFile = "$LogDir\node$i.err"
            if (Test-Path $errFile) {
                $content = Get-Content $errFile -Tail 10 -ErrorAction SilentlyContinue
                if ($content) {
                    Write-Status "Node $i errors:" "Red"
                    $content | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
                }
            }
        }
        throw "Cluster failed to start"
    }
    
    Write-Status "Cluster is ready!" "Green"
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $nodePort = $BasePort + ($i * 10)
        Write-Status "  Node ${i}: 127.0.0.1:${nodePort}" "Gray"
    }
    
    # Wait for Raft leader election to complete
    Write-Status "Waiting for Raft leader election..." "Yellow"
    Start-Sleep -Seconds 5
    
    # Discover the leader by trying to create a test topic on each node
    # The leader will succeed, followers will redirect
    $script:LeaderPort = $null
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $port = $BasePort + ($i * 10)
        Write-Status "  Probing node ${i} at port ${port}..." "Gray"
        
        # Use cargo to run a quick test that creates and deletes a topic
        Push-Location $ProjectDir
        try {
            $env:LANCE_TEST_ADDR = "127.0.0.1:${port}"
            $result = & cargo test --package lnc-client --test integration test_ping_latency -- --ignored --nocapture 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Status "  Node ${i} at port ${port} is responsive (potential leader)" "Green"
                $script:LeaderPort = $port
                break
            }
        } catch {
            # Continue to next node
        } finally {
            Pop-Location
        }
    }
    
    if (-not $script:LeaderPort) {
        Write-Status "Could not determine leader, using node 0" "Yellow"
        $script:LeaderPort = $BasePort
    }
    
    Write-Status "Leader election period complete, leader at port $script:LeaderPort" "Green"
}

function Set-TestEnvironment {
    param([string]$Mode = "cluster")
    
    Write-Status "Setting environment variables..." "Cyan"
    
    # Set environment variables for test discovery
    if ($Mode -eq "cluster") {
        # Use discovered leader port for LANCE_TEST_ADDR
        if ($script:LeaderPort) {
            $env:LANCE_TEST_ADDR = "127.0.0.1:$script:LeaderPort"
        } else {
            $env:LANCE_TEST_ADDR = "127.0.0.1:$BasePort"
        }
        $env:LANCE_NODE1_ADDR = "127.0.0.1:$($BasePort + 0)"    # 1992
        $env:LANCE_NODE2_ADDR = "127.0.0.1:$($BasePort + 10)"   # 2002
        $env:LANCE_NODE3_ADDR = "127.0.0.1:$($BasePort + 20)"   # 2012
    } else {
        $env:LANCE_TEST_ADDR = "127.0.0.1:$BasePort"
        $env:LANCE_NODE1_ADDR = "127.0.0.1:$BasePort"
        $env:LANCE_NODE2_ADDR = "127.0.0.1:$BasePort"
        $env:LANCE_NODE3_ADDR = "127.0.0.1:$BasePort"
    }
    
    Write-Status "  LANCE_TEST_ADDR = $env:LANCE_TEST_ADDR" "Gray"
    Write-Status "  LANCE_NODE1_ADDR = $env:LANCE_NODE1_ADDR" "Gray"
    Write-Status "  LANCE_NODE2_ADDR = $env:LANCE_NODE2_ADDR" "Gray"
    Write-Status "  LANCE_NODE3_ADDR = $env:LANCE_NODE3_ADDR" "Gray"
}

function Run-IntegrationTests {
    param([string]$Mode = "cluster")
    
    Write-Status "Running integration tests (mode: $Mode)..." "Cyan"
    
    $testArgs = @(
        "test",
        "--package", "lnc-client",
        "--test", "integration",
        "--",
        "--ignored",
        "--nocapture"
    )
    
    # Add test filter if specified
    if ($TestFilter) {
        $testArgs += $TestFilter
        Write-Status "  Filter: $TestFilter" "Gray"
    }
    
    # Filter tests based on mode
    if ($Mode -eq "single") {
        Write-Status "  Skipping cluster-specific tests" "Gray"
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
        Write-Status "Integration tests passed!" "Green"
    } else {
        Write-Status "Some integration tests failed (exit code: $script:TestExitCode)" "Red"
    }
}

function Run-UnitTests {
    Write-Status "Running unit tests..." "Cyan"
    
    Push-Location $ProjectDir
    try {
        & cargo test --workspace -- --skip integration
        $script:TestExitCode = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    
    if ($script:TestExitCode -eq 0) {
        Write-Status "Unit tests passed!" "Green"
    } else {
        Write-Status "Unit tests failed (exit code: $script:TestExitCode)" "Red"
    }
    
    return $script:TestExitCode -eq 0
}

function Run-ComplianceChecks {
    Write-Status "Running compliance checks..." "Cyan"
    
    Push-Location $ProjectDir
    try {
        # Check formatting
        Write-Status "  Checking code formatting..." "Gray"
        & cargo fmt --all -- --check
        if ($LASTEXITCODE -ne 0) {
            Write-Status "Code formatting check failed" "Red"
            $script:TestExitCode = 1
            return $false
        }
        
        # Check clippy lints
        Write-Status "  Running clippy..." "Gray"
        & cargo clippy --workspace -- -D warnings
        if ($LASTEXITCODE -ne 0) {
            Write-Status "Clippy check failed" "Red"
            $script:TestExitCode = 1
            return $false
        }
    } finally {
        Pop-Location
    }
    
    Write-Status "Compliance checks passed!" "Green"
    return $true
}

function Clean-TestData {
    Write-Status "Cleaning up test data..." "Yellow"
    
    if (Test-Path $DataDirBase) {
        Remove-Item -Path $DataDirBase -Recurse -Force -ErrorAction SilentlyContinue
    }
    
    Write-Status "Cleanup complete." "Green"
}

# Main execution
$script:TestExitCode = 0
$startTime = Get-Date

try {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  LANCE Integration Test Runner" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Run unit tests first (if not skipped)
    if (-not $SkipUnit) {
        if (-not (Run-UnitTests)) {
            Write-Status "Unit tests failed, stopping." "Red"
            exit $script:TestExitCode
        }
    }
    
    # Run compliance checks (if not skipped)
    if (-not $SkipCompliance) {
        if (-not (Run-ComplianceChecks)) {
            Write-Status "Compliance checks failed, stopping." "Red"
            exit $script:TestExitCode
        }
    }
    
    # Run single-node tests
    if ($TestMode -eq "single" -or $TestMode -eq "both") {
        Write-Host ""
        Write-Host "----------------------------------------" -ForegroundColor Yellow
        Write-Host "  Single-Node Tests (L1 Mode)" -ForegroundColor Yellow
        Write-Host "----------------------------------------" -ForegroundColor Yellow
        
        Start-SingleNode
        Set-TestEnvironment -Mode "single"
        Run-IntegrationTests -Mode "single"
        Stop-AllServers
        Clean-TestData
        
        if ($script:TestExitCode -ne 0 -and $TestMode -eq "both") {
            Write-Status "Single-node tests failed, stopping." "Red"
            exit $script:TestExitCode
        }
    }
    
    # Run cluster tests
    if ($TestMode -eq "cluster" -or $TestMode -eq "both") {
        Write-Host ""
        Write-Host "----------------------------------------" -ForegroundColor Yellow
        Write-Host "  Cluster Tests (L3 Mode - 3 Nodes)" -ForegroundColor Yellow
        Write-Host "----------------------------------------" -ForegroundColor Yellow
        
        Start-Cluster
        Set-TestEnvironment -Mode "cluster"
        Run-IntegrationTests -Mode "cluster"
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    if ($script:TestExitCode -eq 0) {
        Write-Status "All tests passed!" "Green"
    } else {
        Write-Status "Some tests failed." "Red"
    }
    Write-Host "========================================" -ForegroundColor Cyan
    
} catch {
    Write-Status "Error: $_" "Red"
    $script:TestExitCode = 1
} finally {
    # Always stop servers
    Stop-AllServers
    
    # Clean test data
    Clean-TestData
    
    $elapsed = (Get-Date) - $startTime
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Status "Total time: $([Math]::Round($elapsed.TotalSeconds, 1)) seconds" "Cyan"
    Write-Host "========================================" -ForegroundColor Cyan
}

exit $script:TestExitCode
