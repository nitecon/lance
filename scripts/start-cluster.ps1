# LANCE Cluster Startup Script
# Launches 3 LANCE server instances for replication testing
#
# Usage:
#   .\scripts\start-cluster.ps1           # Start cluster
#   .\scripts\start-cluster.ps1 -Stop     # Stop cluster
#   .\scripts\start-cluster.ps1 -Clean    # Stop and clean data directories

param(
    [switch]$Stop,
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

# Configuration
$BasePort = 1992
$BaseMetricsPort = 9090
$BaseHealthPort = 8080
$BaseReplicationPort = 1993
$DataDirBase = "$PSScriptRoot\..\test-data"
$LogDir = "$PSScriptRoot\..\test-logs"
$NodeCount = 3

# Process tracking file
$PidFile = "$PSScriptRoot\..\test-cluster.pids"

function Stop-Cluster {
    Write-Host "Stopping LANCE cluster..." -ForegroundColor Yellow
    
    if (Test-Path $PidFile) {
        $pids = Get-Content $PidFile
        foreach ($pid in $pids) {
            try {
                $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
                if ($process) {
                    Write-Host "  Stopping node (PID: $pid)..."
                    Stop-Process -Id $pid -Force
                }
            } catch {
                # Process already stopped
            }
        }
        Remove-Item $PidFile -Force
    }
    
    # Also kill any lance.exe processes on our ports
    Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | 
        Where-Object { $_.LocalPort -ge $BasePort -and $_.LocalPort -lt ($BasePort + $NodeCount) } |
        ForEach-Object {
            try {
                Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
            } catch {}
        }
    
    Write-Host "Cluster stopped." -ForegroundColor Green
}

function Clean-Data {
    Write-Host "Cleaning data directories..." -ForegroundColor Yellow
    
    if (Test-Path $DataDirBase) {
        Remove-Item -Path $DataDirBase -Recurse -Force
        Write-Host "  Removed $DataDirBase"
    }
    
    if (Test-Path $LogDir) {
        Remove-Item -Path $LogDir -Recurse -Force
        Write-Host "  Removed $LogDir"
    }
    
    Write-Host "Data cleaned." -ForegroundColor Green
}

function Start-Cluster {
    Write-Host "Starting LANCE cluster with $NodeCount nodes..." -ForegroundColor Cyan
    
    # Stop any existing cluster first
    Stop-Cluster
    
    # Create directories
    if (-not (Test-Path $DataDirBase)) {
        New-Item -ItemType Directory -Path $DataDirBase -Force | Out-Null
    }
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    
    # Build the project first
    Write-Host "Building LANCE..." -ForegroundColor Yellow
    Push-Location "$PSScriptRoot\.."
    try {
        cargo build --release
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Build failed!" -ForegroundColor Red
            exit 1
        }
    } finally {
        Pop-Location
    }
    
    $lanceBinary = "$PSScriptRoot\..\target\release\lance.exe"
    if (-not (Test-Path $lanceBinary)) {
        $lanceBinary = "$PSScriptRoot\..\target\debug\lance.exe"
    }
    
    if (-not (Test-Path $lanceBinary)) {
        Write-Host "LANCE binary not found! Run 'cargo build' first." -ForegroundColor Red
        exit 1
    }
    
    # Build peer list
    $peers = @()
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $peers += "127.0.0.1:$($BaseReplicationPort + $i)"
    }
    $peerList = $peers -join ","
    
    $pids = @()
    
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $nodeId = $i
        $port = $BasePort + $i
        $metricsPort = $BaseMetricsPort + $i
        $healthPort = $BaseHealthPort + $i
        $dataDir = "$DataDirBase\node$nodeId"
        $logFile = "$LogDir\node$nodeId.log"
        
        # Create node data directory
        if (-not (Test-Path $dataDir)) {
            New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
        }
        
        $args = @(
            "--node-id", $nodeId,
            "--listen", "127.0.0.1:$port",
            "--metrics", "127.0.0.1:$metricsPort",
            "--health", "127.0.0.1:$healthPort",
            "--data-dir", $dataDir,
            "--replication-mode", "l3",
            "--peers", $peerList
        )
        
        Write-Host "  Starting node $nodeId on port $port..." -ForegroundColor White
        Write-Host "    Peers: $peerList" -ForegroundColor Gray
        
        $process = Start-Process -FilePath $lanceBinary `
            -ArgumentList $args `
            -RedirectStandardOutput $logFile `
            -RedirectStandardError "$LogDir\node$nodeId.err" `
            -PassThru `
            -WindowStyle Hidden
        
        $pids += $process.Id
        Write-Host "    Started (PID: $($process.Id))" -ForegroundColor Green
    }
    
    # Save PIDs for later cleanup
    $pids | Out-File -FilePath $PidFile
    
    # Wait for nodes to start
    Write-Host "`nWaiting for nodes to initialize..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
    
    # Check if nodes are running
    $allRunning = $true
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $port = $BasePort + $i
        try {
            $connection = Test-NetConnection -ComputerName 127.0.0.1 -Port $port -WarningAction SilentlyContinue
            if ($connection.TcpTestSucceeded) {
                Write-Host "  Node $i is listening on port $port" -ForegroundColor Green
            } else {
                Write-Host "  Node $i is NOT responding on port $port" -ForegroundColor Red
                $allRunning = $false
            }
        } catch {
            Write-Host "  Node $i check failed: $_" -ForegroundColor Red
            $allRunning = $false
        }
    }
    
    if ($allRunning) {
        Write-Host "`nCluster is ready!" -ForegroundColor Cyan
        Write-Host "`nNode addresses:" -ForegroundColor White
        for ($i = 0; $i -lt $NodeCount; $i++) {
            Write-Host "  Node $i: 127.0.0.1:$($BasePort + $i)" -ForegroundColor Gray
        }
        Write-Host "`nTo run integration tests against the cluster:" -ForegroundColor White
        Write-Host "  `$env:LANCE_TEST_ADDR='127.0.0.1:$BasePort'" -ForegroundColor Yellow
        Write-Host "  `$env:LANCE_NODE1_ADDR='127.0.0.1:$($BasePort + 0)'" -ForegroundColor Yellow
        Write-Host "  `$env:LANCE_NODE2_ADDR='127.0.0.1:$($BasePort + 1)'" -ForegroundColor Yellow
        Write-Host "  `$env:LANCE_NODE3_ADDR='127.0.0.1:$($BasePort + 2)'" -ForegroundColor Yellow
        Write-Host "  cargo test --package lnc-client --test integration -- --ignored --nocapture" -ForegroundColor Yellow
        Write-Host "`nTo stop the cluster:" -ForegroundColor White
        Write-Host "  .\scripts\start-cluster.ps1 -Stop" -ForegroundColor Yellow
    } else {
        Write-Host "`nSome nodes failed to start. Check logs in $LogDir" -ForegroundColor Red
    }
}

# Main
if ($Stop) {
    Stop-Cluster
} elseif ($Clean) {
    Stop-Cluster
    Clean-Data
} else {
    Start-Cluster
}
