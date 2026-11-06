# Projeto 1 - NYC Yellow Taxi

Estado atual: pipeline completo (M1, M2, M3, O1, O2) em `projeto1.py`, com
validações executadas no Docker a 10% e a 100% dos dados de 2023–2024.
As experiências controladas de desempenho e escala estão documentadas.
A comparação de 10% contra 100% também está concluída (secção 13.6 do
notebook de experiências); a comparação por anos é complementar.
O relatório foi elaborado em LaTeX no Overleaf, seguindo o modelo fornecido.
Neste repositório disponibilizamos apenas a versão final em PDF, em
[relatório_projeto 1.pdf](<relatório_projeto 1.pdf>).
As limitações metodológicas estão descritas no próprio relatório.

| Módulo | O que faz | Saída (em `--output`) |
|---|---|---|
| M1 | Leitura mês a mês, tipos uniformes, limpeza; removidos por critério e por ano | `m1_cleaning_stats.csv` |
| M2 | Viagens, tarifa, duração (min) e distância (km) médias por borough × dia da semana × hora | `m2_demand/` (Parquet por `pickup_borough`) |
| M3 | Zona × mês: média/desvio de tarifa e distância; ranking no borough; quota em 3 meses; anomalia de volume > 1,5σ | `m3_zone_month/` (Parquet por `pickup_borough`) |
| O1 | `fare_per_km` > média + 3σ por (zona, hora); taxas em dias úteis vs fim de semana por pagamento e borough | `o1_anomaly_rates.csv` |
| O2 | Rotas por mês, quota na zona de partida, top-20 por borough, inversão Q1 vs Q3 | `o2_route_month/`, `o2_q1_q3_reversal/`, `o2_top_routes.csv` |

## Ambiente independente

Este diretório contém o seu próprio Compose e Dockerfile. Não precisa da pasta
dos Labs. É necessário ter Docker Desktop com o motor Linux em funcionamento.
Não é necessário criar um ambiente virtual Python no Windows.

O ficheiro `.env` é local: contém `JUPYTER_TOKEN` e não deve ir para o Git.
Para outro membro do grupo, copiar `.env.example` para `.env` e definir um token
próprio antes de arrancar. Nunca publicar esse token no repositório ou relatório.

No PowerShell, na pasta do projeto:

```powershell
Copy-Item .env.example .env
```

Executar esta cópia apenas se ainda não existir um `.env` pessoal. Abrir esse
ficheiro no VS Code e preencher `JUPYTER_TOKEN=` com um valor pessoal não vazio.
Pré-requisitos no PC do colega: Git, VS Code (extensões Python e Jupyter) e
Docker Desktop com contentores Linux. O Python/PySpark será executado no Docker.

Executar no PowerShell, a partir desta pasta:

```powershell
docker compose -f compose.project.yml up -d --build --scale spark-worker=3
docker compose -f compose.project.yml ps
```

O ambiente tem um master, três workers (2 cores e 2 GB por worker) e Jupyter.
Todos partilham o hardware do mesmo PC. A pasta atual aparece como `/project`
no Jupyter e nos workers. As portas publicadas são locais ao PC.
Se o ambiente dos Labs estiver a usar as mesmas portas, pará-lo primeiro com
`docker compose down`, na pasta do Compose dos Labs. Não usar a opção `-v`.

## Dados

Fonte: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Executar `./download_taxi_data.ps1` para obter os 24 meses de 2023 e 2024 e a tabela
de zonas. Os ficheiros ficam em `data/raw` e o manifesto com URLs, tamanhos e
SHA-256 fica em `data/download_manifest.csv`. Não enviar os dados para o Git.

Para descarregar também 2022:

```powershell
.\download_taxi_data.ps1 -Years 2022
```

Cada membro descarrega os dados no seu PC. Se o PowerShell bloquear scripts,
usar `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` no mesmo
terminal; esta definição termina quando o terminal for fechado.

## Notebook no VS Code

Abrir `projeto1.ipynb`, escolher Select Kernel, Select Another Kernel e Existing
Jupyter Server. Indicar `http://localhost:8888/?token=TOKEN_PESSOAL`, usando o valor
do `.env` local, e selecionar Python 3 (ipykernel). Não guardar esse endereço
com o token no Git. Dentro do notebook, os dados de entrada estão em `/project/data/raw`.

- `projeto1.ipynb`: validação com uma amostra de 10%.
- `validacao_completa.ipynb`: as mesmas verificações com 100% dos dados.
- `experiências projeto1.ipynb`: método das experiências e tabelas lidas das métricas guardadas.

Os dois notebooks de validação importam `projeto1.py` e usam pastas de saída separadas. Executar um
de cada vez, desde a preparação até ao fecho da sessão. A secção 8.4 compara
a estabilidade numérica com 12 e 24 partições; não é uma otimização proposta.
Os notebooks de validação conservam os testes e as suas saídas. As comparações
de desempenho estão no notebook de experiências, que lê as métricas guardadas,
apresenta as decisões na secção 14 e mantém o arquivo exploratório na secção 15.
O arquivo preserva código e saídas antigos como consulta, sem executar Spark.
A reorganização preservou as saídas anteriores; não voltou a executar as validações.

## Partilha no Git

O repositório deve conter esta pasta do projeto, sem depender dos Labs.
Partilhar Dockerfile, Compose, `.env.example`, `.gitignore`, `.dockerignore`,
README, script de download e código/notebook. Não partilhar `.env`, `data/`
ou resultados volumosos. O `.gitignore` cobre essas exclusões.
Combinar quem edita o notebook para evitar alterações simultâneas incompatíveis.
Não foi ainda testada a reprodução noutro computador. As tags e bibliotecas
não fixadas podem produzir versões diferentes em builds feitos noutras datas.

## Executar o pipeline

### No Docker (resultados obrigatórios do enunciado)

Teste rápido com 1% dos dados:

```powershell
docker compose -f compose.project.yml exec jupyter /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.driver.host=jupyter --conf spark.driver.bindAddress=0.0.0.0 `
  --executor-memory 1536m `
  /project/projeto1.py --data /project/data/raw --years 2023 2024 `
  --fraction 0.01 --output /project/data/output-teste
```

Para as medições, mudar `--fraction` (0.1, 0.2, 1.0) e `--output` (uma pasta por
corrida). Cada corrida grava `run_info.json` com a configuração e os tempos.

### Localmente no Windows (complemento, Secção 4.5 do relatório)

As medições principais usam Docker. A Secção 4.5 compara a direção dos efeitos
num segundo PC, sem Docker: Windows 11, 16 threads, `local[*]`, Spark 4.2.0
(PySpark num venv com Python 3.12), Java 17 e `winutils.exe`/`hadoop.dll` do
Hadoop 3.4 em `C:\hadoop\bin` (variável `HADOOP_HOME`). Amostra de 10%, uma ou
duas corridas por configuração. Os `run_info.json` de cada corrida estão em
`resultados_local/`. Estas corridas usaram a versão do
`projeto1.py` anterior às correções de arredondamento e à medição com o
profiler; o tempo total é a soma dos blocos medidos.

```powershell
# referência: cache, broadcast, AQE, O1 por groupBy, 200 partições
python projeto1.py --data data\raw --fraction 0.1 --master "local[*]" --driver-memory 8g --output data\local\referencia
# uma opção de cada vez: --no-cache | --no-broadcast | --no-aqe | --o1-method window
# partições: --shuffle-partitions 8 | 16 | 32 | 64 ; dados completos: --fraction 1.0
```

### Argumentos

| Argumento | Significado |
|---|---|
| `--data` | pasta com `yellow/<ano>/*.parquet` e `lookup/taxi_zone_lookup.csv` |
| `--years` | anos a processar (por omissão `2023 2024`) |
| `--fraction` | fração aleatória dos dados, seed fixa (por omissão 1.0) |
| `--output` | pasta de resultados |
| `--modules` | módulos além do M1: `m2 m3 o1 o2` (por omissão todos) |
| `--master` | só fora do spark-submit, ex.: `local[*]` |
| `--driver-memory` | só em execução local, ex.: `8g` |
| `--shuffle-partitions` | `spark.sql.shuffle.partitions` |
| `--no-broadcast` | força merge nas junções com zonas; não desliga o broadcast das estatísticas de O1 |
| `--no-cache` | desliga a cache das viagens limpas |
| `--no-aqe` | desliga AQE |
| `--o1-method` | `groupby` (agregação + broadcast join) ou `window` |

### Convenções e configurações

- Distâncias em km: `trip_distance_km = trip_distance × 1.609344` (o original em milhas mantém-se).
- `spark.sql.session.timeZone = UTC`: as datas da TLC são hora local de NYC sem fuso.
- Limpeza: critérios do enunciado + nulos/NaN + `f_period` (ano do pickup ≠ ano do ficheiro).
- M3: meses sem viagens entram com 0; referência da anomalia = 3 meses anteriores; sem 3 meses de histórico a flag fica nula.
- O2: inversão = sinais opostos de (quota mar − jan) e (quota set − jul), por rota e ano.

Os notebooks de validação importam `projeto1.py` e conferem os resultados,
sem medir desempenho. A pipeline executada por `spark-submit` usa o
`spark_profiler.py`, original da Lab 3. O notebook de experiências lê as métricas guardadas.

Interfaces locais: Spark master em http://localhost:8080 e Jupyter em
http://localhost:8888 (requer o token local). A interface da aplicação Spark
fica em http://localhost:4040 enquanto a aplicação está em execução, se a porta
estiver disponível.

Para parar apenas o ambiente do projeto:

```powershell
docker compose -f compose.project.yml down
```

## Reproduzir as experiências

### Medição da pipeline completa

`run_info.json` regista agora `total_s` desde a leitura até à última escrita,
incluindo os CSV locais. O arranque da sessão Spark e a recolha/exportação das
métricas ficam fora desse tempo e estão identificados separadamente.
`timed_blocks_sum_s` é apenas a soma dos blocos, não o tempo total.
O helper da Lab 3 guarda `profile_metrics.json` (valores numéricos) e
`profile_report.csv` (tabela de leitura).

Referência inicial já medida: sem cache, com broadcast das zonas, AQE ativo,
O1 por groupBy e três workers de dois cores. O1 por groupBy também usa broadcast
das estatísticas; o interruptor `--no-broadcast` controla apenas as zonas.
No teste de cache, mudar só `--no-cache`, mantendo tudo o resto.

Com o ambiente ligado, exemplo para repetir a baseline completa numa pasta nova:

```powershell
docker compose -f compose.project.yml exec -T jupyter /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=jupyter --conf spark.driver.bindAddress=0.0.0.0 --executor-memory 1536m /project/projeto1.py --data /project/data/raw --years 2023 2024 --fraction 1.0 --no-cache --shuffle-partitions 200 --output /project/data/experiencias/nova_baseline_sem_cache_01
```

Cada repetição deve usar uma aplicação Spark nova e uma pasta própria. Não
executar notebooks em paralelo. Registar as condições do PC; o cache de ficheiros
do sistema operativo pode persistir entre aplicações e não é a cache do Spark.
O teste de funcionamento com `--fraction 0.01` não é a baseline final.

O hardware observado, os limites do Docker e os IDs das imagens estão em
`experiências docker/resultados/ambiente_escala.json`. As configurações e as medições
estão nos ficheiros de cada corrida e no notebook de experiências.
Na revisão final da entrega, conferir também as versões das bibliotecas e a
reprodução do ambiente. A imagem base `spark:python3` segue os Labs, mas a tag
não fixa uma versão imutável.

### Repetições e opções

Fazer três execuções independentes (`_01`, `_02`, `_03`), fechando outras sessões
Spark. Usar sempre uma pasta nova: a pipeline escreve com `overwrite`.
O comando acima é a baseline sem cache. Para cada alternativa, manter os restantes
argumentos e mudar só a opção indicada:

| Experiência | Opções da pipeline |
|---|---|
| Referência com cache | retirar `--no-cache`; manter `--shuffle-partitions 200` |
| O1 com janela | referência com cache + `--o1-method window` |
| Zonas por merge | referência com cache + `--no-broadcast` |
| Sem AQE | referência com cache + `--no-aqe` |
| Shuffle 12 ou 24 | referência com cache, trocar `200` por `12` ou `24` |
| Amostra | referência com cache, trocar `--fraction 1.0` por `--fraction 0.1` |
| Anos (complementar) | referência com cache, trocar `--years` por `2023` ou `2022 2023 2024` |

A amostra usa seed 42 depois da união dos ficheiros; continua a ler os 24 meses.
A comparação de workers usa as mesmas opções da referência com cache e altera
apenas o número de workers. O runner `experiências docker/executar_escala.ps1` documenta
as três rondas 1/2/3/6 e repõe três workers no fim. Não o executar por cima de
resultados existentes. Para alterar manualmente, com as aplicações Spark fechadas:

```powershell
docker compose -f compose.project.yml up -d --no-deps --no-build --scale spark-worker=2 spark-worker
```

Confirmar os workers na interface do master antes da próxima corrida. No fim,
repetir com `--scale spark-worker=3`.

### Conferir e partilhar resultados

O verificador seguinte compara as sete tabelas de três corridas com a referência,
alinhando chaves e usando tolerância apenas nos valores de ponto flutuante.
A comparação corre depois das medições e precisa dos Parquet locais:

```powershell
docker compose -f compose.project.yml exec -T jupyter python3 "/project/experiências docker/verificar_resultados.py" --prefix baseline_com_cache --reference baseline_sem_cache_01 --report comparacao_cache_reproduzida.json
```

Para corridas novas, adaptar os nomes em `--prefix` e `--reference`. Os verificadores
`verificar_amostra.py` e `verificar_dimensao.py` conferem os conjuntos históricos
identificados nos próprios scripts. Não executam novas medições.

Copiar apenas os ficheiros pequenos de cada corrida para o diretório de métricas:

```powershell
$runName = "nova_baseline_sem_cache_01"
$metricsPath = Join-Path "experiências docker/resultados" $runName
New-Item -ItemType Directory -Force -Path $metricsPath | Out-Null
foreach ($metricFile in @("run_info.json", "profile_metrics.json", "profile_report.csv", "m1_cleaning_stats.csv")) {
    Copy-Item -LiteralPath (Join-Path "data/experiencias/$runName" $metricFile) -Destination $metricsPath
}
```

O notebook de experiências lê os resultados históricos incluídos no repositório.
Uma corrida com outro nome não substitui automaticamente uma comparação: é preciso
selecioná-la nas células correspondentes. É possível consultar as tabelas e gráficos
sem descarregar os Parquet; repetir os cálculos e validações exige os dados originais.

Os URLs exatos e hashes dos ficheiros usados estão em
`experiências docker/resultados/download_manifest.csv`, copiado do manifesto local.
O script constrói os URLs mensais oficiais como
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet`
(trocar ano e mês), e a tabela de zonas em
`https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.
Os hashes permitem detetar alterações posteriores na fonte; não garantem que a TLC
não volte a publicar uma versão diferente.

Versões observadas no Jupyter: Python 3.10.12, Spark 4.2.0, pandas 2.3.3,
NumPy 2.2.6, PyArrow 25.0.1, Matplotlib 3.10.9, Py4J 0.10.9.9 e JupyterLab 4.6.5.
Não alterámos nem reconstruímos o ambiente medido nesta revisão. Como o Dockerfile
usa uma tag e dependências não fixadas, um build futuro pode diferir. A reprodução
noutro PC e um build imutável permanecem limitações, não testes concluídos.
