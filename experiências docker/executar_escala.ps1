# Correr na pasta do projeto, com Docker ligado e sem notebooks Spark ativos.
param(
    [int[]]$WorkerCounts = @(1, 2, 3, 6),
    [int]$Repetitions = 3
)

$ErrorActionPreference = 'Stop'
$scaleProjectRoot = Split-Path -Parent $PSScriptRoot
$scaleOriginalLocation = Get-Location
Set-Location -LiteralPath $scaleProjectRoot
$scaleLog = Join-Path $scaleProjectRoot 'data/experiencias/escala_control.log'

function Get-ScaleMasterState {
    Invoke-RestMethod -Uri 'http://localhost:8080/json/' -TimeoutSec 10
}

function Wait-ScaleReady([int]$ExpectedWorkers) {
    $scaleDeadline = (Get-Date).AddSeconds(45)
    do {
        $scaleState = Get-ScaleMasterState
        if ($scaleState.activeapps.Count -eq 0 -and
            $scaleState.aliveworkers -eq $ExpectedWorkers -and
            $scaleState.cores -eq (2 * $ExpectedWorkers)) {
            return $scaleState
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $scaleDeadline)
    throw "O cluster nao ficou pronto com $ExpectedWorkers workers."
}

# Recusar pastas existentes antes de alterar o ambiente.
foreach ($scaleRepeat in 1..$Repetitions) {
    foreach ($scaleWorkers in $WorkerCounts) {
        $scaleName = 'escala_{0}w_cache_{1:D2}' -f $scaleWorkers, $scaleRepeat
        if (Test-Path -LiteralPath (Join-Path 'data/experiencias' $scaleName)) {
            throw "A pasta ja existe: $scaleName. Nao sobrepor medicoes."
        }
    }
}

try {
    foreach ($scaleRepeat in 1..$Repetitions) {
        foreach ($scaleWorkers in $WorkerCounts) {
            $scaleName = 'escala_{0}w_cache_{1:D2}' -f $scaleWorkers, $scaleRepeat
            $scaleStateBefore = Get-ScaleMasterState
            if ($scaleStateBefore.activeapps.Count -ne 0) {
                throw 'Ha uma aplicacao Spark ativa; nao alterar os workers.'
            }
            Write-Output "PREPARAR $scaleName"
            docker compose -f compose.project.yml up -d --no-deps --no-build --scale "spark-worker=$scaleWorkers" spark-worker >> $scaleLog 2>&1
            if ($LASTEXITCODE -ne 0) { throw 'Falhou a alteracao do numero de workers.' }
            $scaleState = Wait-ScaleReady $scaleWorkers
            $scaleFolder = Join-Path 'data/experiencias' $scaleName
            New-Item -ItemType Directory -Path $scaleFolder | Out-Null
            [ordered]@{
                run = $scaleName
                workers = $scaleWorkers
                cores_per_worker = 2
                worker_memory_GiB = 2
                executor_memory = '1536m'
                total_cores = $scaleState.cores
                started_local = (Get-Date).ToString('o')
            } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $scaleFolder 'cluster_info.json') -Encoding utf8
            Write-Output "INICIO $scaleName : $($scaleState.cores) cores"
            docker compose -f compose.project.yml exec -T jupyter /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=jupyter --conf spark.driver.bindAddress=0.0.0.0 --executor-memory 1536m /project/projeto1.py --data /project/data/raw --years 2023 2024 --fraction 1.0 --shuffle-partitions 200 --output "/project/data/experiencias/$scaleName" *> (Join-Path $scaleFolder 'terminal.log')
            if ($LASTEXITCODE -ne 0) { throw "Falhou $scaleName" }
            $scaleInfo = Get-Content -Raw -LiteralPath (Join-Path $scaleFolder 'run_info.json') | ConvertFrom-Json
            if ($scaleInfo.parallelism -ne (2 * $scaleWorkers)) {
                throw "Paralelismo inesperado em $scaleName"
            }
            Write-Output "FIM $scaleName : $($scaleInfo.total_s) s; viagens $($scaleInfo.clean_rows)"
        }
    }
} finally {
    # Repor três workers apenas se isso não interromper outra aplicação.
    $scaleFinalState = Get-ScaleMasterState
    if ($scaleFinalState.activeapps.Count -eq 0) {
        docker compose -f compose.project.yml up -d --no-deps --no-build --scale spark-worker=3 spark-worker >> $scaleLog 2>&1
        if ($LASTEXITCODE -ne 0) { throw 'Falhou a reposicao dos tres workers.' }
        $scaleRestored = Wait-ScaleReady 3
        Write-Output "AMBIENTE REPOSTO: $($scaleRestored.aliveworkers) workers"
    } else {
        Write-Output 'Reposicao adiada: ha uma aplicacao Spark ativa.'
    }
    Set-Location -LiteralPath $scaleOriginalLocation
}
