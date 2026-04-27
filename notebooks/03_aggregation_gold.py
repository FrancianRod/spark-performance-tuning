# =============================================================================
# CAMADA GOLD — Refined / Analytical Layer
# spark-lakehouse-pipeline | Francian Rodrigues
# =============================================================================
# CONTEXTO DE NEGÓCIO:
#   Esta camada entrega os KPIs estratégicos prontos para consumo em Power BI.
#   Replica o cenário real onde dashboards executivos aceleraram a tomada de
#   decisão em 40% — eliminando o processo manual de consolidação semanal.
#
#   Tabelas geradas:
#     1. gold_sales_by_region      → Performance comercial por região/período
#     2. gold_sales_by_category    → Análise de mix e margem por categoria
#     3. gold_logistics_kpi        → SLA de entrega e eficiência logística
#     4. gold_executive_summary    → Visão consolidada para C-Level (join vendas+logística)
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("lakehouse-gold-aggregation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Otimização: broadcast join automático para tabelas < 10MB (dimensões pequenas)
    .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# CAMINHOS
# ---------------------------------------------------------------------------
BASE_PATH = "data"

PATHS = {
    "silver_sales":         f"{BASE_PATH}/silver/sales",
    "silver_logistics":     f"{BASE_PATH}/silver/logistics",
    "gold_sales_region":    f"{BASE_PATH}/gold/sales_by_region",
    "gold_sales_category":  f"{BASE_PATH}/gold/sales_by_category",
    "gold_logistics_kpi":   f"{BASE_PATH}/gold/logistics_kpi",
    "gold_exec_summary":    f"{BASE_PATH}/gold/executive_summary",
}

# ---------------------------------------------------------------------------
# 1. LEITURA DA CAMADA SILVER
# ---------------------------------------------------------------------------

def read_silver(path: str) -> DataFrame:
    logger.info(f"Lendo Silver: {path}")
    return spark.read.format("delta").load(path)


# ---------------------------------------------------------------------------
# 2. GOLD TABLE 1 — Vendas por Região e Período
# ---------------------------------------------------------------------------

def build_sales_by_region(df: DataFrame) -> DataFrame:
    """
    KPI Central: Performance de vendas por região e período.
    Alimenta o painel comercial executivo no Power BI.

    OTIMIZAÇÃO — groupBy + agg em vez de múltiplos withColumn:
      Consolidar todas as métricas em um único groupBy evita múltiplos
      shuffles (operações de rede entre workers). Cada shuffle adicional
      em dados de GB+ pode adicionar minutos ao tempo de execução.
    """
    logger.info("Construindo gold_sales_by_region...")

    df_agg = (
        df
        .groupBy("region", "order_year", "order_month", "order_quarter")
        .agg(
            # Volume financeiro
            F.round(F.sum("net_sales_amount"),  2).alias("total_net_revenue"),
            F.round(F.sum("sales_amount"),      2).alias("total_gross_revenue"),
            F.round(F.avg("sales_amount"),      2).alias("avg_order_value"),
            F.round(F.avg("discount"),          2).alias("avg_discount_pct"),

            # Volume operacional
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),

            # Métricas derivadas de eficiência
            F.round(F.sum("net_sales_amount") / F.countDistinct("customer_id"), 2)
             .alias("revenue_per_customer"),
        )
    )

    # Ranking de regiões por receita líquida dentro de cada período
    # Window Function: calculada de forma distribuída, sem trazer dados para o driver
    window_rank = Window.partitionBy("order_year", "order_month").orderBy(
        F.col("total_net_revenue").desc()
    )
    df_agg = df_agg.withColumn("region_rank_by_revenue", F.rank().over(window_rank))

    # Crescimento MoM (Month over Month) por região
    window_mom = Window.partitionBy("region").orderBy("order_year", "order_month")
    df_agg = (
        df_agg
        .withColumn("prev_month_revenue", F.lag("total_net_revenue", 1).over(window_mom))
        .withColumn(
            "mom_growth_pct",
            F.round(
                (F.col("total_net_revenue") - F.col("prev_month_revenue"))
                / F.col("prev_month_revenue") * 100,
                2
            )
        )
        .drop("prev_month_revenue")
    )

    df_agg = df_agg.withColumn("_gold_timestamp", F.current_timestamp())
    logger.info(f"gold_sales_by_region: {df_agg.count()} linhas")
    return df_agg


# ---------------------------------------------------------------------------
# 3. GOLD TABLE 2 — Vendas por Categoria de Produto
# ---------------------------------------------------------------------------

def build_sales_by_category(df: DataFrame) -> DataFrame:
    """
    Análise de mix de produtos: quais categorias geram mais receita e margem.
    Suporta decisões de portfolio e precificação.
    """
    logger.info("Construindo gold_sales_by_category...")

    df_agg = (
        df
        .groupBy("product_category", "order_year", "order_quarter")
        .agg(
            F.round(F.sum("net_sales_amount"),  2).alias("total_net_revenue"),
            F.round(F.avg("net_sales_amount"),  2).alias("avg_net_revenue_per_order"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.avg("discount"),          2).alias("avg_discount_pct"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )

    # Share de receita por categoria dentro do trimestre (% do total)
    window_total = Window.partitionBy("order_year", "order_quarter")
    df_agg = df_agg.withColumn(
        "revenue_share_pct",
        F.round(
            F.col("total_net_revenue") /
            F.sum("total_net_revenue").over(window_total) * 100,
            2
        )
    )

    df_agg = df_agg.withColumn("_gold_timestamp", F.current_timestamp())
    logger.info(f"gold_sales_by_category: {df_agg.count()} linhas")
    return df_agg


# ---------------------------------------------------------------------------
# 4. GOLD TABLE 3 — KPIs de Logística / SLA
# ---------------------------------------------------------------------------

def build_logistics_kpi(df: DataFrame) -> DataFrame:
    """
    Performance logística: SLA, lead time médio e custo por transportadora.
    KPI crítico para negociações com carriers e gestão de supply chain.
    """
    logger.info("Construindo gold_logistics_kpi...")

    df_agg = (
        df
        .groupBy("carrier", "status")
        .agg(
            F.count("shipment_id").alias("total_shipments"),

            # SLA Performance
            F.round(F.avg("delivery_lead_time_days"), 1).alias("avg_lead_time_days"),
            F.round(F.min("delivery_lead_time_days"), 1).alias("min_lead_time_days"),
            F.round(F.max("delivery_lead_time_days"), 1).alias("max_lead_time_days"),

            # Taxa de entrega no prazo
            F.round(
                F.sum(F.col("delivered_on_time").cast("int")) /
                F.count("shipment_id") * 100,
                2
            ).alias("on_time_delivery_rate_pct"),

            # Custo logístico
            F.round(F.sum("freight_cost"),  2).alias("total_freight_cost"),
            F.round(F.avg("freight_cost"),  2).alias("avg_freight_cost_per_shipment"),
        )
    )

    # Flag de performance: transportadora abaixo de 80% de SLA
    df_agg = df_agg.withColumn(
        "carrier_performance_flag",
        F.when(F.col("on_time_delivery_rate_pct") < 80, F.lit("ABAIXO_SLA"))
         .when(F.col("on_time_delivery_rate_pct") < 95, F.lit("ATENCAO"))
         .otherwise(F.lit("OK"))
    )

    df_agg = df_agg.withColumn("_gold_timestamp", F.current_timestamp())
    logger.info(f"gold_logistics_kpi: {df_agg.count()} linhas")
    return df_agg


# ---------------------------------------------------------------------------
# 5. GOLD TABLE 4 — Executive Summary (JOIN Vendas + Logística)
# ---------------------------------------------------------------------------

def build_executive_summary(df_sales: DataFrame, df_logistics: DataFrame) -> DataFrame:
    """
    Visão consolidada: combina performance comercial com eficiência logística.
    Esta é a tabela principal do dashboard executivo no Power BI.

    OTIMIZAÇÃO DE JOIN:
      - Fazemos o join em order_id que está presente em ambas as tabelas Silver.
      - left join: mantém todos os pedidos mesmo sem dados de entrega (pedidos pendentes).
      - Para escala (bilhões de linhas), adicionar .hint("broadcast") na tabela menor
        ou usar BUCKET JOIN caso ambas estejam em tabelas Delta catalogadas.
    """
    logger.info("Construindo gold_executive_summary (join vendas + logística)...")

    # Agrega logística por pedido antes do join (evita explosão de linhas)
    df_logistics_by_order = (
        df_logistics
        .groupBy("order_id")
        .agg(
            F.round(F.avg("delivery_lead_time_days"), 1).alias("avg_lead_time"),
            F.round(F.sum("freight_cost"), 2).alias("total_freight_cost"),
            F.max("delivered_on_time").alias("delivered_on_time"),
            F.first("carrier").alias("primary_carrier"),
            F.first("status").alias("shipment_status"),
        )
    )

    # JOIN principal: vendas (Silver) + logística agregada
    df_joined = (
        df_sales
        .join(df_logistics_by_order, on="order_id", how="left")
    )

    # Agregação final por região e período
    df_exec = (
        df_joined
        .groupBy("region", "order_year", "order_month", "order_quarter")
        .agg(
            # Financeiro
            F.round(F.sum("net_sales_amount"),  2).alias("total_net_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),

            # Operacional
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.avg("discount"), 2).alias("avg_discount_pct"),

            # Logística
            F.round(F.avg("avg_lead_time"), 1).alias("avg_delivery_lead_time"),
            F.round(F.sum("total_freight_cost"), 2).alias("total_freight_cost"),
            F.round(
                F.sum(F.col("delivered_on_time").cast("int")) /
                F.count("order_id") * 100,
                2
            ).alias("on_time_delivery_rate_pct"),

            # Custo logístico como % da receita
            F.round(
                F.sum("total_freight_cost") / F.sum("net_sales_amount") * 100,
                2
            ).alias("freight_cost_as_pct_revenue"),
        )
    )

    df_exec = df_exec.withColumn("_gold_timestamp", F.current_timestamp())
    logger.info(f"gold_executive_summary: {df_exec.count()} linhas")
    return df_exec


# ---------------------------------------------------------------------------
# 6. ESCRITA NA CAMADA GOLD
# ---------------------------------------------------------------------------

def write_gold(df: DataFrame, output_path: str, partition_cols: list = None):
    """
    A Gold é escrita com OVERWRITE COMPLETO pois é uma camada de análise,
    não de histórico. Cada execução recalcula os agregados desde a Silver.

    PARTICIONAMENTO NA GOLD:
      Granularidade menor que na Silver: particionamos por ano (não mês),
      pois queries no Power BI geralmente acessam um ou dois anos inteiros.
      Particionar demais aqui causaria small files desnecessários.
    """
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info(f"Gold salva em: {output_path}")


# ---------------------------------------------------------------------------
# 7. EXECUÇÃO PRINCIPAL
# ---------------------------------------------------------------------------

def run():
    logger.info("=" * 60)
    logger.info("GOLD LAYER — Iniciando construção de tabelas analíticas")
    logger.info("=" * 60)

    # Leitura das Silvers
    df_sales      = read_silver(PATHS["silver_sales"])
    df_logistics  = read_silver(PATHS["silver_logistics"])

    # Cache estratégico: df_sales é lido 3x → cache em memória evita 3 scans no disco
    # Em clusters com RAM limitada, use .persist(StorageLevel.DISK_ONLY) como fallback
    df_sales.cache()
    df_sales.count()  # Materializa o cache
    logger.info("df_sales cacheado em memória para reutilização nas 3 tabelas Gold.")

    # Constrói e persiste cada tabela Gold
    write_gold(
        build_sales_by_region(df_sales),
        PATHS["gold_sales_region"],
        partition_cols=["order_year"]
    )

    write_gold(
        build_sales_by_category(df_sales),
        PATHS["gold_sales_category"],
        partition_cols=["order_year"]
    )

    write_gold(
        build_logistics_kpi(df_logistics),
        PATHS["gold_logistics_kpi"]
    )

    write_gold(
        build_executive_summary(df_sales, df_logistics),
        PATHS["gold_exec_summary"],
        partition_cols=["order_year"]
    )

    # Libera cache após uso
    df_sales.unpersist()

    logger.info("=" * 60)
    logger.info("GOLD LAYER — Pipeline completo. Dados prontos para Power BI.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
