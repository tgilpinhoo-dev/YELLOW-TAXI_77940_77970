# Executar no PowerShell. Dados oficiais NYC TLC; nao instala dependencias.
param([int[]]$Years = @(2024, 2023))
$ErrorActionPreference = 'Stop'
$dataRoot = Join-Path $PSScriptRoot 'data'
New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null
$manifest = [System.Collections.Generic.List[object]]::new()
$manifestPath = Join-Path $dataRoot 'download_manifest.csv'
if (Test-Path -LiteralPath $manifestPath) {
    foreach ($entry in (Import-Csv -LiteralPath $manifestPath)) {
        $manifest.Add($entry)
    }
}

function Test-ParquetFile([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $stream = [System.IO.File]::OpenRead($Path)
    try {
        if ($stream.Length -lt 8) { return $false }
        $bytes = New-Object byte[] 4
        [void]$stream.Read($bytes, 0, 4)
        $first = [Text.Encoding]::ASCII.GetString($bytes)
        [void]$stream.Seek(-4, [IO.SeekOrigin]::End)
        [void]$stream.Read($bytes, 0, 4)
        return ($first -eq 'PAR1' -and [Text.Encoding]::ASCII.GetString($bytes) -eq 'PAR1')
    } finally { $stream.Dispose() }
}

function Get-DataFile([string]$Url, [string]$Destination, [bool]$Parquet) {
    $valid = if ($Parquet) { Test-ParquetFile $Destination } else { Test-Path -LiteralPath $Destination }
    if (-not $valid) {
        $partial = "$Destination.partial"
        $success = $false
        for ($attempt = 1; $attempt -le 3; $attempt++) {
            try {
                Write-Host "A descarregar $(Split-Path $Destination -Leaf) (tentativa $attempt)..."
                Invoke-WebRequest -Uri $Url -OutFile $partial -UseBasicParsing
                if ($Parquet -and -not (Test-ParquetFile $partial)) { throw 'Ficheiro Parquet incompleto ou invalido.' }
                if (-not $Parquet) {
                    $rows = @(Import-Csv -LiteralPath $partial)
                    if ($rows.Count -eq 0 -or -not ($rows[0].PSObject.Properties.Name -contains 'LocationID')) { throw 'Tabela de zonas invalida.' }
                }
                Move-Item -LiteralPath $partial -Destination $Destination -Force
                $success = $true
                break
            } catch {
                if ($attempt -eq 3) { throw }
                Write-Warning $_.Exception.Message
                Start-Sleep -Seconds 3
            }
        }
        if (-not $success) { throw "Falha no download: $Url" }
    } else { Write-Host "Ja existe: $(Split-Path $Destination -Leaf)" }
    $item = Get-Item -LiteralPath $Destination
    $relativeFile = $item.FullName.Substring($dataRoot.Length + 1)
    foreach ($oldEntry in @($manifest | Where-Object { $_.file -eq $relativeFile })) {
        [void]$manifest.Remove($oldEntry)
    }
    $manifest.Add([pscustomobject]@{
        file = $relativeFile
        url = $Url
        bytes = $item.Length
        sha256 = (Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash
        checked_utc = [DateTime]::UtcNow.ToString('o')
    })
    $manifest | Export-Csv -LiteralPath (Join-Path $dataRoot 'download_manifest.csv') -NoTypeInformation -Encoding UTF8
}

foreach ($year in $Years) {
    if ($year -notin @(2022, 2023, 2024)) { throw 'Anos suportados: 2022, 2023 e 2024.' }
    $folder = Join-Path $dataRoot "raw/yellow/$year"
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    foreach ($month in 1..12) {
        $name = 'yellow_tripdata_{0}-{1:00}.parquet' -f $year, $month
        Get-DataFile "https://d37ci6vzurychx.cloudfront.net/trip-data/$name" (Join-Path $folder $name) $true
    }
}
$lookup = Join-Path $dataRoot 'raw/lookup'
New-Item -ItemType Directory -Force -Path $lookup | Out-Null
Get-DataFile 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv' (Join-Path $lookup 'taxi_zone_lookup.csv') $false
Write-Host "Concluido para os anos pedidos: $($Years -join ', '). Manifesto com $($manifest.Count) ficheiros em $dataRoot/download_manifest.csv"
