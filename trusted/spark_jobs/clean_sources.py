"""Spark-backed Trusted Zone cleaning for high-volume structured sources."""

from __future__ import annotations

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.spark_session import build_spark_session


SPARK_TRUSTED_TABLES = ("kev", "epss", "nvd")


def run_spark_trusted_cleaning(table_names: tuple[str, ...] = SPARK_TRUSTED_TABLES) -> dict[str, int | str]:
    """Clean selected silver tables with Spark and write Trusted Delta outputs."""
    spark = build_spark_session("cybersecintel-trusted-cleaning")
    try:
        source = DeltaLakeStorage.from_env()
        trusted = DeltaLakeStorage.from_env_bucket("TRUSTED_DELTA_BUCKET", "trusted")
        summary: dict[str, int | str] = {"engine": "spark"}
        for table_name in table_names:
            dataframe = _read_delta_or_empty(spark, source, table_name)
            cleaned = _clean_table_spark(dataframe, table_name)
            _write_delta(cleaned, trusted, table_name, partition_by=["ingest_date"])
            summary[table_name] = cleaned.count()
        return summary
    finally:
        spark.stop()


def _clean_table_spark(dataframe, table_name: str):
    from pyspark.sql import functions as F

    if table_name == "kev":
        return (
            dataframe
            .withColumn("cve_id", F.upper(F.coalesce(_col(dataframe, "cve_id"), _col(dataframe, "cveID"))))
            .where(F.col("cve_id").isNotNull() & (F.col("cve_id") != ""))
            .dropDuplicates(["cve_id"])
            .withColumn("ingest_date", F.coalesce(_col(dataframe, "ingest_date"), F.current_date().cast("string")))
            .withColumn("_quality_warn", F.lit(False))
            .withColumn("_quality_reasons", F.lit(""))
        )
    if table_name == "epss":
        return (
            dataframe
            .withColumn("cve_id", F.upper(F.coalesce(_col(dataframe, "cve_id"), _col(dataframe, "cve"))))
            .withColumn("epss_score", F.coalesce(_col(dataframe, "epss_score"), _col(dataframe, "epss")).cast("double"))
            .withColumn("epss_percentile", F.coalesce(_col(dataframe, "epss_percentile"), _col(dataframe, "percentile")).cast("double"))
            .where(F.col("cve_id").isNotNull() & (F.col("cve_id") != ""))
            .dropDuplicates(["cve_id", "date"])
            .withColumn("ingest_date", F.coalesce(_col(dataframe, "ingest_date"), F.current_date().cast("string")))
            .withColumn("_quality_warn", (F.col("epss_score") < 0) | (F.col("epss_score") > 1))
            .withColumn("_quality_reasons", F.when(F.col("_quality_warn"), F.lit("epss outside [0,1]")).otherwise(F.lit("")))
        )
    if table_name == "nvd":
        return (
            dataframe
            .withColumn("cve_id", F.upper(_col(dataframe, "cve_id")))
            .withColumn("cvss_v3_score", F.coalesce(_col(dataframe, "cvss_v3_score"), _col(dataframe, "cvss_score")).cast("double"))
            .where(F.col("cve_id").isNotNull() & (F.col("cve_id") != ""))
            .dropDuplicates(["cve_id"])
            .withColumn("ingest_date", F.coalesce(_col(dataframe, "ingest_date"), F.current_date().cast("string")))
            .withColumn("_quality_warn", (F.col("cvss_v3_score") < 0) | (F.col("cvss_v3_score") > 10))
            .withColumn("_quality_reasons", F.when(F.col("_quality_warn"), F.lit("cvss outside [0,10]")).otherwise(F.lit("")))
        )
    return dataframe


def _read_delta_or_empty(spark, storage: DeltaLakeStorage, table_name: str):
    from pyspark.sql.types import StringType, StructField, StructType

    try:
        return spark.read.format("delta").load(storage.table_uri(table_name, spark=True))
    except Exception:
        return spark.createDataFrame([], StructType([StructField("_empty", StringType(), True)]))


def _col(dataframe, name: str):
    from pyspark.sql import functions as F

    if name in dataframe.columns:
        return F.col(name)
    return F.lit(None).cast("string")


def _write_delta(dataframe, storage: DeltaLakeStorage, table_name: str, *, partition_by: list[str] | None = None) -> None:
    writer = dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(storage.table_uri(table_name, spark=True))
