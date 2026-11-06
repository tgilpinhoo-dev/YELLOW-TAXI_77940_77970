"""
projeto1.py - Large-Scale Analytics with Spark (ComPar 2026/27)
================================================================
Pipeline PySpark sobre os dados NYC TLC Yellow Taxi (2023 e 2024).

  M1  ingestão + limpeza, com removidos por critério e por ano
  M2  procura por (pickup_borough, dia da semana, hora) -> Parquet por borough
  M3  zona x mês: médias/desvios + Window (ranking no borough, quota em 3 meses,
      flag de volume > 1.5 sigma da média dos 3 meses anteriores)
  O1  tarifas anómalas: fare_per_km > média + 3 sigma por (zona, hora);
      taxas em dias úteis vs fim de semana por payment_type e borough
  O2  rotas origem-destino por mês: quota na zona de partida, top-20 por
      borough e inversão de tendência entre Q1 e Q3

Convenções que adotámos (descritas também no README):
  - distâncias em km: trip_distance_km = trip_distance x 1.609344
    (trip_distance original, em milhas, mantém-se)
  - colunas novas em inglês; comentários em português

Execução local (Windows):
    python projeto1.py --data C:/ComPar/TAXI_AMARELO/data/raw --years 2023 2024 ^
        --output C:/ComPar/TAXI_AMARELO/data/output --master local[*] --driver-memory 8g

Execução no Docker (ver README):
    docker compose -f compose.project.yml exec jupyter /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --conf spark.driver.host=jupyter \
        --conf spark.driver.bindAddress=0.0.0.0 --executor-memory 1536m \
        /project/projeto1.py --data /project/data/raw --years 2023 2024 --output /project/data/output
"""

import argparse
import glob
import json
import os
import time
from functools import reduce

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from spark_profiler import SparkProfiler

MILES_TO_KM = 1.609344
MODULES = ["m2", "m3", "o1", "o2"]   # o M1 corre sempre (os outros dependem dele)

# ---------------------------------------------------------------------------
# Colunas lidas e tipo comum. Ler só estas = column pruning (Parquet é colunar).
# ---------------------------------------------------------------------------
TRIP_COLS = {
    "tpep_pickup_datetime":  TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count":       IntegerType(),
    "trip_distance":         DoubleType(),
    "PULocationID":          IntegerType(),
    "DOLocationID":          IntegerType(),
    "payment_type":          IntegerType(),
    "fare_amount":           DoubleType(),
}

# Critérios do enunciado + nulos + período (ver add_quality_flags)
QUALITY_FLAGS = ["f_nulls", "f_fare", "f_distance", "f_passengers",
                 "f_time", "f_location", "f_period", "f_any"]


# ===========================================================================
# M1 - Ingestão
# ===========================================================================
def find_trip_files(data_folder, year):
    """Aceita a estrutura do grupo (data/raw/yellow/<ano>/) ou uma pasta plana."""
    for pattern in (os.path.join(data_folder, "yellow", str(year), f"yellow_tripdata_{year}-*.parquet"),
                    os.path.join(data_folder, f"yellow_tripdata_{year}-*.parquet")):
        files = sorted(glob.glob(pattern))
        if files:
            return files
    raise FileNotFoundError(f"Nenhum ficheiro de {year} em {data_folder}")


def read_trips(spark, data_folder, years, fraction=1.0, seed=42):
    """Lê os ficheiros mensais dos anos pedidos, uniformiza tipos e junta-os.

    - Cada mês é lido separadamente: a TLC nem sempre usa o mesmo tipo físico
      para a mesma coluna (ex.: passenger_count double vs int64).
    - try_cast em vez de cast: o Spark 4 usa modo ANSI e um cast impossível
      (ex.: NaN -> int) faria o job falhar; assim fica null e é contado em f_nulls.
    - source_year guarda o ano do FICHEIRO, para distinguir do ano do timestamp.
    - unionByName junta por NOME de coluna (não por posição).
    """
    monthly, n_files = [], 0
    for year in years:
        for f in find_trip_files(data_folder, year):
            n_files += 1
            monthly.append(
                spark.read.parquet(f)
                     .select([sf.col(c).try_cast(t).alias(c) for c, t in TRIP_COLS.items()])
                     .withColumn("source_year", sf.lit(year)))
    trips = reduce(lambda a, b: a.unionByName(b), monthly)
    if fraction < 1.0:
        trips = trips.sample(fraction=fraction, seed=seed)   # amostra reprodutível
    return trips, n_files


def read_zones(spark, data_folder):
    """Tabela de zonas (265 linhas): LocationID -> Borough, Zone, service_zone."""
    path = os.path.join(data_folder, "lookup", "taxi_zone_lookup.csv")
    if not os.path.exists(path):
        path = os.path.join(data_folder, "taxi_zone_lookup.csv")
    return (spark.read.option("header", True).option("inferSchema", True).csv(path)
            .select(sf.col("LocationID").cast("int"), "Borough", "Zone", "service_zone"))


# ===========================================================================
# M1 - Limpeza
# ===========================================================================
def add_quality_flags(df):
    """Uma coluna booleana por critério de exclusão (True = registo inválido).

    f_nulls  : nulo ou NaN num campo indispensável. Em Spark, `null <= 0` dá
               null e o where() descartaria a linha em silêncio; assim todas as
               removidas ficam contabilizadas num critério com nome.
    f_period : ano do pickup diferente do ano do ficheiro (relógio do
               taxímetro errado, ex.: 2002). Não está na lista do enunciado:
               é uma decisão do grupo, reportada à parte.
    f_any    : total real de removidas (uma viagem pode falhar vários critérios).
    """
    def ok(cond):
        return sf.coalesce(cond, sf.lit(False))   # null já foi contado em f_nulls

    def missing(c, t):
        cond = sf.col(c).isNull()
        return cond | sf.isnan(c) if isinstance(t, DoubleType) else cond

    any_null = reduce(lambda a, b: a | b, [missing(c, t) for c, t in TRIP_COLS.items()])

    return (df
        .withColumn("f_nulls",      any_null)
        .withColumn("f_fare",       ok(sf.col("fare_amount") <= 0))
        .withColumn("f_distance",   ok(sf.col("trip_distance") <= 0))
        .withColumn("f_passengers", ok(~sf.col("passenger_count").between(1, 6)))
        .withColumn("f_time",       ok(sf.col("tpep_dropoff_datetime")
                                       <= sf.col("tpep_pickup_datetime")))
        .withColumn("f_location",   ok(~sf.col("PULocationID").between(1, 263)
                                       | ~sf.col("DOLocationID").between(1, 263)))
        .withColumn("f_period",     ok(sf.year("tpep_pickup_datetime") != sf.col("source_year")))
        .withColumn("f_any", reduce(lambda a, b: a | b,
                                    [sf.col(f) for f in QUALITY_FLAGS if f != "f_any"])))


def cleaning_stats(flagged):
    """Removidos por critério e por ano (do ficheiro) - UMA só passagem."""
    return (flagged
        .groupBy("source_year")
        .agg(sf.count("*").alias("total"),
             *[sf.sum(sf.col(f).cast("int")).alias(f) for f in QUALITY_FLAGS])
        .withColumn("kept", sf.col("total") - sf.col("f_any"))
        .orderBy("source_year"))


def clean_trips(flagged):
    """Viagens válidas + colunas derivadas usadas pelos módulos seguintes."""
    return (flagged
        .where(~sf.col("f_any"))
        .drop(*QUALITY_FLAGS)
        .withColumn("trip_distance_km", sf.col("trip_distance") * MILES_TO_KM)
        .withColumn("trip_duration_minutes",
                    (sf.unix_timestamp("tpep_dropoff_datetime")
                     - sf.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .withColumn("pickup_year",  sf.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", sf.month("tpep_pickup_datetime"))
        .withColumn("day_of_week",  sf.dayofweek("tpep_pickup_datetime"))   # 1=Dom ... 7=Sáb
        .withColumn("hour_of_day",  sf.hour("tpep_pickup_datetime")))


# ===========================================================================
# Junção com as zonas (usada por M2, M3, O1 e O2)
# ===========================================================================
def zone_side(zones, side, use_broadcast=True):
    """Tabela de zonas renomeada para um dos lados da viagem ("pickup"/"dropoff").

    O O2 junta as zonas DUAS vezes; nomes distintos por lado evitam colunas
    ambíguas. Com broadcast, cada executor recebe as 265 linhas e a tabela de
    viagens não sofre shuffle para o join.
    """
    key = "PULocationID" if side == "pickup" else "DOLocationID"
    z = zones.select(sf.col("LocationID").alias(key),
                     sf.col("Borough").alias(f"{side}_borough"),
                     sf.col("Zone").alias(f"{side}_zone"))
    return sf.broadcast(z) if use_broadcast else z.hint("merge")


def add_pickup_location(trips, zones, use_broadcast=True):
    return trips.join(zone_side(zones, "pickup", use_broadcast), on="PULocationID", how="inner")


# ===========================================================================
# M2 - procura por borough x dia da semana x hora
# ===========================================================================
def m2_demand(trips_pu):
    return (trips_pu
        .groupBy("pickup_borough", "day_of_week", "hour_of_day")
        .agg(sf.count("*").alias("trip_count"),
             # Decimal estabiliza a média monetária nos limites de arredondamento.
             # Seis casas nas entradas; a coluna fare_amount original mantém-se.
             sf.round(sf.avg(sf.col("fare_amount").cast("decimal(20, 6)")), 2)
               .alias("mean_fare_amount"),
             # Somar segundos em decimal evita diferenças junto ao arredondamento.
             sf.round(sf.avg(
                 (sf.unix_timestamp("tpep_dropoff_datetime")
                  - sf.unix_timestamp("tpep_pickup_datetime"))
                 .cast("decimal(20, 0)")
             ) / 60, 2).alias("mean_trip_duration_minutes"),
             sf.round(sf.avg("trip_distance_km"), 3).alias("mean_trip_distance_km")))


# ===========================================================================
# M3 - zona x mês com Window functions
# ===========================================================================
def m3_zone_month(spark, trips_pu, zones, years, sigma=1.5):
    """Estatísticas por zona de partida e mês + três métricas com janelas.

    Convenções (plano do grupo):
    - month_index = ano*12 + mês: dez/2023 e jan/2024 são meses seguidos, por
      isso o histórico de 2023 alimenta as janelas do início de 2024.
    - Meses sem viagens entram com trip_count = 0 (grelha zona x mês completa):
      três linhas = três meses consecutivos.
    - rank_in_borough : dense_rank por volume dentro de (borough, mês).
    - share_3m        : viagens da zona / viagens do borough, nos meses [m-2, m].
    - rolling_mean_3m / rolling_std_3m: média e desvio dos 3 meses ANTERIORES
      ([m-3, m-1]); o mês atual não entra na própria referência.
    - volume_anomaly  : |volume - média| > 1.5 x desvio; null quando não há 3
      meses de histórico (insuficiente). Com desvio 0, qualquer variação conta.
    """
    monthly = (trips_pu
        .groupBy("PULocationID", "pickup_year", "pickup_month")
        .agg(sf.count("*").alias("trip_count"),
             # A mesma precisão monetária de M2, antes de arredondar.
             sf.round(sf.avg(sf.col("fare_amount").cast("decimal(20, 6)")), 2)
               .alias("mean_fare_amount"),
             sf.round(sf.stddev("fare_amount"), 2).alias("std_fare_amount"),
             sf.round(sf.avg("trip_distance_km"), 3).alias("mean_trip_distance_km"),
             sf.round(sf.stddev("trip_distance_km"), 3).alias("std_trip_distance_km")))

    # grelha completa: 263 zonas x todos os meses dos anos pedidos (pequena)
    months = spark.createDataFrame([(y, m) for y in years for m in range(1, 13)],
                                   ["pickup_year", "pickup_month"])
    grid = (zones.where(sf.col("LocationID").between(1, 263))
                 .select(sf.col("LocationID").alias("PULocationID"),
                         sf.col("Borough").alias("pickup_borough"),
                         sf.col("Zone").alias("pickup_zone"))
                 .crossJoin(months))

    full = (grid.join(monthly, ["PULocationID", "pickup_year", "pickup_month"], "left")
                .fillna({"trip_count": 0})
                .withColumn("month_index", sf.col("pickup_year") * 12 + sf.col("pickup_month")))

    w_rank       = Window.partitionBy("pickup_borough", "month_index").orderBy(sf.desc("trip_count"))
    w_zone_3m    = Window.partitionBy("PULocationID").orderBy("month_index").rangeBetween(-2, 0)
    w_borough_3m = Window.partitionBy("pickup_borough").orderBy("month_index").rangeBetween(-2, 0)
    w_prev_3m    = Window.partitionBy("PULocationID").orderBy("month_index").rangeBetween(-3, -1)

    return (full
        .withColumn("rank_in_borough", sf.dense_rank().over(w_rank))
        # try_divide: em modo ANSI, divisão por zero faria o job falhar
        .withColumn("share_3m", sf.round(sf.try_divide(sf.sum("trip_count").over(w_zone_3m),
                                                       sf.sum("trip_count").over(w_borough_3m)), 5))
        .withColumn("rolling_mean_3m", sf.avg("trip_count").over(w_prev_3m))
        .withColumn("rolling_std_3m",  sf.stddev("trip_count").over(w_prev_3m))
        .withColumn("n_prev_months",   sf.count("trip_count").over(w_prev_3m))
        .withColumn("volume_anomaly",
                    sf.when(sf.col("n_prev_months") == 3,
                            sf.abs(sf.col("trip_count") - sf.col("rolling_mean_3m"))
                            > sigma * sf.col("rolling_std_3m")))
        .withColumn("rolling_mean_3m", sf.round("rolling_mean_3m", 1))
        .withColumn("rolling_std_3m",  sf.round("rolling_std_3m", 1))
        .drop("n_prev_months"))


# ===========================================================================
# O1 - tarifas anómalas
# ===========================================================================
def o1_fare_anomalies(trips_pu, method="groupby", k=3.0):
    """Sinaliza viagens com fare_per_km > média + k*sigma do seu bucket
    (PULocationID, hour_of_day) e calcula taxas de anomalia.

    method="window" : média/sigma com Window sobre cada bucket -> obriga a
                      shuffle + ordenação de TODAS as viagens por bucket.
    method="groupby": agrega primeiro (~263x24 linhas), depois broadcast join
                      de volta às viagens -> sem shuffle da tabela grande no join.
    Os dois dão o mesmo resultado: é uma das experiências de otimização.
    """
    trips = trips_pu.withColumn("fare_per_km",
                                sf.try_divide(sf.col("fare_amount"), sf.col("trip_distance_km")))
    bucket = ["PULocationID", "hour_of_day"]

    if method == "window":
        w = Window.partitionBy(*bucket)
        with_stats = (trips
            .withColumn("bucket_mean", sf.avg("fare_per_km").over(w))
            .withColumn("bucket_std",  sf.stddev("fare_per_km").over(w)))
    else:
        stats = (trips.groupBy(*bucket)
                      .agg(sf.avg("fare_per_km").alias("bucket_mean"),
                           sf.stddev("fare_per_km").alias("bucket_std")))
        with_stats = trips.join(sf.broadcast(stats), on=bucket, how="inner")

    flagged = (with_stats
        .withColumn("fare_anomaly", sf.coalesce(
            sf.col("fare_per_km") > sf.col("bucket_mean") + k * sf.col("bucket_std"),
            sf.lit(False)))     # bucket com 1 viagem: sigma null -> não anómalo
        .withColumn("day_type", sf.when(sf.col("day_of_week").isin(1, 7), "weekend")
                                  .otherwise("weekday")))

    # taxa = anómalas / viagens do PRÓPRIO grupo (não confundir grupo grande com anómalo)
    rates = (flagged
        .groupBy("day_type", "payment_type", "pickup_borough")
        .agg(sf.count("*").alias("trips"),
             sf.sum(sf.col("fare_anomaly").cast("int")).alias("anomalies"))
        .withColumn("anomaly_rate_pct", sf.round(100 * sf.col("anomalies") / sf.col("trips"), 3))
        .orderBy("pickup_borough", "payment_type", "day_type"))
    return rates


# ===========================================================================
# O2 - rotas origem-destino
# ===========================================================================
def o2_routes(clean, zones, use_broadcast=True, top_n=20):
    """Rotas (PU, DO) por mês: quota na zona de partida, top-N por borough e
    inversão de tendência entre Q1 e Q3.

    - Dois joins às zonas, com nomes distintos por lado (pickup_*, dropoff_*).
    - Chave mensal inclui o ano: jan/2023 e jan/2024 nunca se misturam.
    - route_share = viagens da rota / viagens que saem da zona de partida no mês.
    - Top-N por borough de partida: por total de viagens no período; desempate
      determinístico por (PULocationID, DOLocationID).
    - Inversão Q1 vs Q3 (definição operacional do grupo): por rota e ano,
      delta_q1 = quota(mar) - quota(jan); delta_q3 = quota(set) - quota(jul);
      inversão se os sinais forem opostos. Mês sem viagens na rota = quota 0.
    """
    route_month = (clean
        .groupBy("PULocationID", "DOLocationID", "pickup_year", "pickup_month")
        .agg(sf.count("*").alias("trip_count")))

    w_out = Window.partitionBy("PULocationID", "pickup_year", "pickup_month")
    route_month = (route_month
        .withColumn("route_share", sf.col("trip_count") / sf.sum("trip_count").over(w_out))
        .join(zone_side(zones, "pickup", use_broadcast), "PULocationID")
        .join(zone_side(zones, "dropoff", use_broadcast), "DOLocationID"))

    # inversão Q1 vs Q3 (agregação condicional: uma linha por rota e ano)
    def share_in(m):
        return sf.coalesce(sf.sum(sf.when(sf.col("pickup_month") == m, sf.col("route_share"))),
                           sf.lit(0.0))
    reversal = (route_month
        .groupBy("PULocationID", "DOLocationID", "pickup_year")
        .agg(share_in(1).alias("s_jan"), share_in(3).alias("s_mar"),
             share_in(7).alias("s_jul"), share_in(9).alias("s_sep"))
        .withColumn("delta_q1", sf.col("s_mar") - sf.col("s_jan"))
        .withColumn("delta_q3", sf.col("s_sep") - sf.col("s_jul"))
        .withColumn("q1_q3_reversal", sf.col("delta_q1") * sf.col("delta_q3") < 0))

    # top-N por borough de partida (total do período)
    totals = (route_month
        .groupBy("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
                 "PULocationID", "DOLocationID")
        .agg(sf.sum("trip_count").alias("total_trips")))
    w_top = Window.partitionBy("pickup_borough").orderBy(
        sf.desc("total_trips"), "PULocationID", "DOLocationID")
    n_rev = (reversal.groupBy("PULocationID", "DOLocationID")
                     .agg(sf.sum(sf.col("q1_q3_reversal").cast("int")).alias("years_with_reversal")))
    top = (totals
        .withColumn("route_rank", sf.row_number().over(w_top))
        .where(sf.col("route_rank") <= top_n)
        .join(n_rev, ["PULocationID", "DOLocationID"], "left")
        .orderBy("pickup_borough", "route_rank"))

    return route_month, reversal, top


# ===========================================================================
# Execução
# ===========================================================================
def build_spark(args):
    builder = (SparkSession.builder
               .appName("taxi-pipeline-" + "-".join(map(str, args.years)))
               # datas da TLC = hora local de NYC sem fuso; UTC evita deslocamentos
               # pelo fuso da máquina (ex.: Europe/Lisbon) nas mudanças de hora
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.adaptive.enabled", str(not args.no_aqe).lower())
               .config("spark.ui.showConsoleProgress", "false"))
    if args.master:
        builder = builder.master(args.master)
    if args.driver_memory:
        # só tem efeito com "python projeto1.py"; com spark-submit usar --driver-memory
        builder = builder.config("spark.driver.memory", args.driver_memory)
    if args.shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", args.shuffle_partitions)
    return builder.getOrCreate()


def timed(label, timings, fn):
    """Corre fn(), mede o tempo de parede e guarda-o em timings[label]."""
    t0 = time.perf_counter()
    result = fn()
    timings[label] = round(time.perf_counter() - t0, 2)
    print(f"[{label}] {timings[label]} s")
    return result


def write_parquet(df, path, partition_col=None):
    w = df.write.mode("overwrite")
    if partition_col:
        w = w.partitionBy(partition_col)
    w.parquet(path)


def main():
    p = argparse.ArgumentParser(description="NYC Yellow Taxi - pipeline Spark (ComPar 2026/27)")
    p.add_argument("--data", default="/project/data/raw",
                   help="pasta com yellow/<ano>/*.parquet e lookup/taxi_zone_lookup.csv")
    p.add_argument("--years", type=int, nargs="+", default=[2023, 2024])
    p.add_argument("--fraction", type=float, default=1.0, help="fração dos dados (0-1]")
    p.add_argument("--output", default="/project/data/output")
    p.add_argument("--modules", nargs="+", default=MODULES, choices=MODULES,
                   help="módulos a correr além do M1 (por omissão todos)")
    p.add_argument("--master", default=None, help="ex.: local[*] (com spark-submit, omitir)")
    p.add_argument("--driver-memory", default=None, help="ex.: 8g (só em execução local)")
    p.add_argument("--shuffle-partitions", type=int, default=None)
    # Alterar uma opção de cada vez nas experiências.
    p.add_argument("--no-broadcast", action="store_true", help="força SortMergeJoin com as zonas")
    p.add_argument("--no-cache", action="store_true", help="não faz cache da tabela limpa")
    p.add_argument("--no-aqe", action="store_true", help="desliga Adaptive Query Execution")
    p.add_argument("--o1-method", default="groupby", choices=["groupby", "window"])
    args = p.parse_args()
    if not 0 < args.fraction <= 1:
        p.error("--fraction deve estar no intervalo (0, 1].")
    if args.shuffle_partitions is not None and args.shuffle_partitions <= 0:
        p.error("--shuffle-partitions deve ser um inteiro positivo.")
    if len(args.years) != len(set(args.years)):
        p.error("--years não deve repetir anos: isso duplicaria as viagens.")

    startup_started = time.perf_counter()
    spark = build_spark(args)
    startup_s = time.perf_counter() - startup_started
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print(f"Spark {spark.version} | master={sc.master} | parallelism={sc.defaultParallelism}")
    print(f"Spark UI: {sc.uiWebUrl}")
    os.makedirs(args.output, exist_ok=True)
    out = lambda name: os.path.join(args.output, name)
    timings = {}
    use_bc = not args.no_broadcast

    profiler = SparkProfiler(spark)
    # Medir desde a leitura até à última escrita, sem cache de outra aplicação.
    with profiler.profile("pipeline"):
        pipeline_started = time.perf_counter()
        # ---- M1 ---------------------------------------------------------------
        trips, n_files = read_trips(spark, args.data, args.years, args.fraction)
        zones = read_zones(spark, args.data)
        flagged = add_quality_flags(trips)

        stats = timed("M1_stats", timings, lambda: cleaning_stats(flagged).toPandas())
        print(stats.to_string(index=False))
        stats.to_csv(out("m1_cleaning_stats.csv"), index=False)

        clean = clean_trips(flagged)
        if not args.no_cache:
            clean = clean.cache()   # reutilizada por M2, M3, O1 e O2
        n_clean = timed("M1_clean_count", timings, clean.count)
        print(f"viagens válidas: {n_clean}")

        trips_pu = add_pickup_location(clean, zones, use_bc)

        # ---- M2 ---------------------------------------------------------------
        if "m2" in args.modules:
            timed("M2_write", timings,
                  lambda: write_parquet(m2_demand(trips_pu), out("m2_demand"), "pickup_borough"))

        # ---- M3 ---------------------------------------------------------------
        if "m3" in args.modules:
            timed("M3_write", timings,
                  lambda: write_parquet(m3_zone_month(spark, trips_pu, zones, args.years),
                                        out("m3_zone_month"), "pickup_borough"))

        # ---- O1 ---------------------------------------------------------------
        if "o1" in args.modules:
            rates = timed("O1_rates", timings,
                          lambda: o1_fare_anomalies(trips_pu, args.o1_method).toPandas())
            rates.to_csv(out("o1_anomaly_rates.csv"), index=False)

        # ---- O2 ---------------------------------------------------------------
        if "o2" in args.modules:
            route_month, reversal, top = o2_routes(clean, zones, use_bc)
            timed("O2_routes_write", timings, lambda: write_parquet(route_month, out("o2_route_month")))
            timed("O2_reversal_write", timings, lambda: write_parquet(reversal, out("o2_q1_q3_reversal")))
            top_pd = timed("O2_top", timings, lambda: top.toPandas())
            top_pd.to_csv(out("o2_top_routes.csv"), index=False)
        pipeline_s = time.perf_counter() - pipeline_started

    # ---- resumo da corrida (para as tabelas do relatório) ------------------
    run_info = {"years": args.years, "fraction": args.fraction, "files": n_files,
                "modules": ["m1"] + args.modules, "master": sc.master,
                "parallelism": sc.defaultParallelism, "spark_version": spark.version,
                "broadcast": use_bc, "cache": not args.no_cache, "aqe": not args.no_aqe,
                "o1_method": args.o1_method, "clean_rows": n_clean, "timings_s": timings,
                "total_s": round(pipeline_s, 3),
                "timed_blocks_sum_s": round(sum(timings.values()), 2),
                "spark_startup_s": round(startup_s, 3),
                "total_scope": "leitura ate ultima escrita; exclui arranque Spark e recolha de metricas",
                "spark_application_id": sc.applicationId,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "executor_memory": sc.getConf().get("spark.executor.memory", "default"),
                "broadcast_scope": "joins com zonas; O1 groupby usa broadcast das estatisticas"}
    if "pipeline" not in profiler.runs:
        raise RuntimeError("Não foi possível recolher as métricas; não usar esta corrida na comparação.")
    with open(out("profile_metrics.json"), "w") as fh:
        json.dump(profiler.runs["pipeline"], fh, indent=2)
    profiler.report().to_csv(out("profile_report.csv"))
    print(json.dumps(run_info, indent=2))
    with open(out("run_info.json"), "w") as fh:
        json.dump(run_info, fh, indent=2)

    if not args.no_cache:
        clean.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
