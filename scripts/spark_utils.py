"""
spark_utils.py
==============
Funções utilitárias reutilizáveis entre as camadas do Lakehouse.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "lakehouse-pipeline") -> SparkSession:
    """
    Factory de SparkSession com configurações padrão do projeto.
    Em ambiente Databricks, retorna a sessão já existente (spark.builder.getOrCreate()).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .getOrCreate()
    )


def profile_dataframe(df: DataFrame, name: str = "DataFrame") -> None:
    """
    Exibe perfil básico de qualidade: contagem, nulos por coluna e schema.
    Útil para validação visual rápida após cada transformação.
    """
    total = df.count()
    logger.info(f"\n{'='*50}")
    logger.info(f"PERFIL: {name} | {total} registros | {len(df.columns)} colunas")
    logger.info(f"{'='*50}")

    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    for col, nulls in null_counts.items():
        pct = (nulls / total * 100) if total > 0 else 0
        flag = " ⚠" if pct > 5 else ""
        logger.info(f"  {col:40s} nulls: {nulls:6d} ({pct:5.1f}%){flag}")


def optimize_delta_table(spark: SparkSession, path: str, z_order_cols: list = None) -> None:
    """
    Executa OPTIMIZE + ZORDER em uma tabela Delta para eliminar small files
    e melhorar performance de leitura para queries filtradas.

    Deve ser executado periodicamente via job agendado (ex: diário/semanal),
    não a cada ingestão — é uma operação de manutenção, não de pipeline.

    Args:
        z_order_cols: colunas usadas frequentemente em filtros WHERE/JOIN.
                      Ex: ["region", "order_year"] para a Gold de vendas.
    """
    optimize_sql = f"OPTIMIZE delta.`{path}`"
    if z_order_cols:
        z_cols = ", ".join(z_order_cols)
        optimize_sql += f" ZORDER BY ({z_cols})"

    logger.info(f"Executando: {optimize_sql}")
    spark.sql(optimize_sql)
    logger.info(f"OPTIMIZE concluído: {path}")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Remove arquivos físicos antigos não referenciados pelo Delta Log.
    retention_hours=168 equivale a 7 dias (padrão recomendado pela Databricks).

    ATENÇÃO: reduzir abaixo de 7 dias pode causar falha em leituras concorrentes.
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"VACUUM concluído: {path} | Retenção: {retention_hours}h")
