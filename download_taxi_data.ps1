<#
.SYNOPSIS
  Descarrega os NYC TLC Yellow Taxi Trip Records (Parquet) e a tabela de zonas.

.DESCRIPTION
  Estrutura criada (relativa a esta pasta):
    data/raw/yellow/<ano>/yellow_tripdata_<ano>-MM.parquet
    data/raw/lookup/taxi_zone_lookup.csv
    data/download_manifest.csv   (ficheiro, URL, bytes, SHA-256)

  - Ficheiros existentes e válidos (cabeçalho e rodapé "PAR1") são reutilizados:
    pode voltar a executar-se depois de uma falha.
  - Downloads incompletos ficam com a extensão .partial e são descartados.
  - Esta verificação é básica; a validação real é feita pelo Spark (M1).
  Não instala programas. Usa curl.exe, incluído no Windows 10/11.

.EXAMPLE
  .\download_taxi_data.ps1                 # 2024 e 2023
  .\download_taxi_data.ps1 -Years 2022     # acrescenta 2022
#>
param(
    [int[]]$Years = @(2024, 2023),
    [string]$DataDir = (Join-Path $PSScriptRoot "data")
)

$ErrorActionPreference = "Stop"
$base     = "https://d37ci6vzurychx.cloudfront.net"
$raw      = Join-Path $DataDir "raw"
$manifest = Join-Path $DataDir "download_manifest.csv"

function Test-Parquet([string]$path) {
    # Um ficheiro Parquet começa e termina com os 4 bytes "PAR1"
    $fs = [System.IO.File]::OpenRead($path)
    try {
        if ($fs.Length -lt 8) { return $false }
        $head = New-Object byte[] 4
        $tail = New-Object byte[] 4
        [void]$fs.Read($head, 0, 4)
        [void]$fs.Seek(-4, [System.IO.SeekOrigin]::End)
        [void]$fs.Read($tail, 0, 4)
        $ascii = [System.Text.Encoding]::ASCII
        return ($ascii.GetString($head) -eq "PAR1") -and ($ascii.GetString($tail) -eq "PAR1")
    } finally { $fs.Close() }
}

function Save-Url([string]$url, [string]$dest) {
    $partial = "$dest.partial"
    curl.exe -L -f -s -S -o $partial $url
    if ($LASTEXITCODE -ne 0) {
        Remove-Item $partial -ErrorAction SilentlyContinue
        throw "Falhou o download de $url (curl devolveu $LASTEXITCODE)"
    }
    Move-Item -Force $partial $dest
}

function Get-ManifestRow([string]$path, [string]$url) {
    [pscustomobject]@{
        file   = (Split-Path $path -Leaf)
        url    = $url
        bytes  = (Get-Item $path).Length
        sha256 = (Get-FileHash $path -Algorithm SHA256).Hash
    }
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
$rows = @()

# ---- viagens: um ficheiro por mês ------------------------------------------
foreach ($y in $Years) {
    $dir = Join-Path $raw (Join-Path "yellow" "$y")
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    foreach ($m in 1..12) {
        $name = "yellow_tripdata_{0}-{1:D2}.parquet" -f $y, $m
        $dest = Join-Path $dir $name
        $url  = "$base/trip-data/$name"
        if ((Test-Path $dest) -and (Test-Parquet $dest)) {
            Write-Host "$name já existe e é válido - reutilizado"
        } else {
            Write-Host "A descarregar $name ..."
            Save-Url $url $dest
            if (-not (Test-Parquet $dest)) { throw "$name não tem o formato Parquet esperado" }
        }
        $rows += Get-ManifestRow $dest $url
    }
}

# ---- tabela de zonas --------------------------------------------------------
$lookupDir = Join-Path $raw "lookup"
New-Item -ItemType Directory -Force -Path $lookupDir | Out-Null
$lookup    = Join-Path $lookupDir "taxi_zone_lookup.csv"
$lookupUrl = "$base/misc/taxi_zone_lookup.csv"
if (-not (Test-Path $lookup)) {
    Write-Host "A descarregar taxi_zone_lookup.csv ..."
    Save-Url $lookupUrl $lookup
}
$rows += Get-ManifestRow $lookup $lookupUrl

# ---- manifesto: junta ao existente (substitui linhas do mesmo ficheiro) -----
if (Test-Path $manifest) {
    $novos = $rows | ForEach-Object { $_.file }
    $rows  = @(Import-Csv $manifest | Where-Object { $novos -notcontains $_.file }) + $rows
}
$rows | Sort-Object file | Export-Csv -NoTypeInformation -Encoding UTF8 $manifest

Write-Host ""
Write-Host "Concluído: $($rows.Count) ficheiros no manifesto ($manifest)"
