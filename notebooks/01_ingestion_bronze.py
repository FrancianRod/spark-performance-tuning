# =============================================================================
# CAMADA BRONZE — Raw Ingestion
# spark-lakehouse-pipeline | Francian Rodrigues
# =============================================================================
# CONTEXTO DE NEGÓCIO:
#   Pipeline inspirado em cenário real: consolidação de indicadores estratégicos
#   de vendas e logística que antes demandava processamento manual semanal.
#   A automação desta camada eliminou ~90% do tempo de consolidação operacional.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. INICIALIZAÇÃO DA SESSÃO SPARK
# ---------------------------------------------------------------------------
# Em produção (Databricks/EMR), a SparkSession já existe no contexto do cluster.
# Para execução local, configuramos com Delta Lake e parâmetros de otimização.

spark = (
    SparkSession.builder
    .appName("lakehouse-bronze-ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Otimização: evita criação excessiva de small files durante escrita paralela
    .config("spark.sql.shuffle.partitions", "200")
    # Habilita inferência de schema com merge para fontes heterogêneas
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info("SparkSession inicializada com suporte a Delta Lake.")

# ---------------------------------------------------------------------------
# 2. DEFINIÇÃO DE SCHEMAS EXPLÍCITOS
# ---------------------------------------------------------------------------
# Schema Enforcement: definir schemas explicitamente (em vez de inferir) é uma
# prática sênior que garante consistência entre ingestões e evita type drift.

SCHEMA_VENDAS = StructType([
    StructField("order_id",         StringType(),    nullable=False),
    StructField("order_date",       StringType(),    nullable=True),   # Raw: string → normalizado na Silver
    StructField("customer_id",      StringType(),    nullable=True),
    StructField("product_id",       StringType(),    nullable=True),
    StructField("product_category", StringType(),    nullable=True),
    StructField("region",           StringType(),    nullable=True),
    StructField("sales_amount",     StringType(),    nullable=True),   # Raw: pode conter "$", "," etc.
    StructField("quantity",         StringType(),    nullable=True),
    StructField("discount",         StringType(),    nullable=True),
    StructField("salesperson_id",   StringType(),    nullable=True),
])

SCHEMA_LOGISTICA = StructType([
    StructField("shipment_id",      StringType(),    nullable=False),
    StructField("order_id",         StringType(),    nullable=True),
    StructField("dispatch_date",    StringType(),    nullable=True),
    StructField("delivery_date",    StringType(),    nullable=True),
    StructField("carrier",          StringType(),    nullable=True),
    StructField("origin_city",      StringType(),    nullable=True),
    StructField("destination_city", StringType(),    nullable=True),
    StructField("freight_cost",     StringType(),    nullable=True),
    StructField("status",           StringType(),    nullable=True),
])

# ---------------------------------------------------------------------------
# 3. CAMINHOS (configurável por ambiente: local / Databricks DBFS / S3 / ADLS)
# ---------------------------------------------------------------------------
BASE_PATH = "data"   # Em Databricks: "dbfs:/mnt/lakehouse" ou "abfss://..."

PATHS = {
    "sales_csv":      f"{BASE_PATH}/raw/sales_data.csv",
    "logistics_json": f"{BASE_PATH}/raw/logistics_data.json",
    "bronze_sales":   f"{BASE_PATH}/bronze/sales",
    "bronze_logistics": f"{BASE_PATH}/bronze/logistics",
}

# ---------------------------------------------------------------------------
# 4. FUNÇÕES DE INGESTÃO
# ---------------------------------------------------------------------------

def ingest_sales_csv(path: str, schema: StructType):
    """
    Lê dados brutos de vendas em CSV com schema explícito.
    Adiciona metadados de auditoria: data de ingestão e nome do arquivo fonte.
    """
    logger.info(f"Iniciando ingestão de vendas: {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .schema(schema)
        .csv(path)
    )

    # Metadados de auditoria — rastreabilidade por camada (padrão de governança)
    from pyspark.sql.functions import current_timestamp, lit, input_file_name
    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file",         input_file_name())
        .withColumn("_layer",               lit("bronze"))
    )

    logger.info(f"Vendas lidas: {df.count()} registros | Colunas: {len(df.columns)}")
    return df


def ingest_logistics_json(path: str, schema: StructType):
    """
    Lê dados brutos de logística em JSON (multiline ou JSON Lines).
    Formato JSON é comum em integrações com APIs REST e sistemas de rastreamento.
    """
    logger.info(f"Iniciando ingestão de logística: {path}")

    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")      # PERMISSIVE: registros malformados viram null (auditáveis)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .json(path)
    )

    from pyspark.sql.functions import current_timestamp, lit, input_file_name
    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file",         input_file_name())
        .withColumn("_layer",               lit("bronze"))
    )

    logger.info(f"Logística lida: {df.count()} registros | Colunas: {len(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# 5. FUNÇÃO DE ESCRITA COM PARTICIONAMENTO OTIMIZADO
# ---------------------------------------------------------------------------

def write_bronze(df, output_path: str, partition_cols: list = None):
    """
    Persiste dados brutos em formato Delta Lake com particionamento estratégico.

    OTIMIZAÇÃO — partitionBy:
      Particionar por ano/mês garante que queries futuras (Silver/Gold) leiam
      apenas as partições relevantes (partition pruning), evitando full scans.
      Sem particionamento, cada worker leria o dataset inteiro → gargalo em TBs.

    SMALL FILES PROBLEM:
      Em ingestões incrementais frequentes, cada micro-batch cria arquivos
      pequenos que degradam performance de leitura. A solução aqui é combinar:
      - partitionBy com granularidade adequada (ano/mês, não dia)
      - Delta Lake OPTIMIZE + ZORDER (executar periodicamente via job agendado)
    """
    writer = (
        df.write
        .format("delta")
        .mode("append")                    # append: preserva histórico completo na Bronze
        .option("mergeSchema", "true")     # tolerante a evolução de schema entre ingestões
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info(f"Bronze salva em: {output_path} | Partições: {partition_cols}")


# ---------------------------------------------------------------------------
# 6. EXECUÇÃO PRINCIPAL
# ---------------------------------------------------------------------------

def run():
    logger.info("=" * 60)
    logger.info("BRONZE LAYER — Iniciando pipeline de ingestão")
    logger.info("=" * 60)

    # --- Vendas ---
    df_sales = ingest_sales_csv(PATHS["sales_csv"], SCHEMA_VENDAS)
    df_sales.printSchema()
    df_sales.show(5, truncate=False)

    # Particionamento por ano de ingestão para suporte a queries temporais
    from pyspark.sql.functions import year, current_date
    df_sales = df_sales.withColumn("_year", year(current_date()))

    write_bronze(df_sales, PATHS["bronze_sales"], partition_cols=["_year"])

    # --- Logística ---
    df_logistics = ingest_logistics_json(PATHS["logistics_json"], SCHEMA_LOGISTICA)
    df_logistics.printSchema()
    df_logistics.show(5, truncate=False)

    df_logistics = df_logistics.withColumn("_year", year(current_date()))
    write_bronze(df_logistics, PATHS["bronze_logistics"], partition_cols=["_year"])

    logger.info("BRONZE LAYER — Ingestão concluída com sucesso.")
    logger.info("Próximo passo: executar 02_transformation_silver.py")


if __name__ == "__main__":
    run()
