"""
Transform - EMR Serverless + PySpark

Le os dados brutos (bronze) do S3, aplica transformacoes
e salva no S3 (silver) em formato Parquet - particionado.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round as spark_round,
    to_date, year, month
)
import sys

def transform(input_path, output_path):
    spark = SparkSession.builder \
        .appName("indicadores-transform") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Lendo dados de: {input_path}")

    # multiline=True porque cada arquivo bronze e um array JSON
    # (formato [{...}, {...}]) e nao um objeto por linha (NDJSON)
    df = spark.read.option("multiline", "true").json(input_path)

    df.printSchema()

    df = df.withColumn("valor_double", col("valor").cast("double")) \
           .withColumn("data_parsed", to_date(col("data"), "dd/MM/yyyy"))

    df_silver = df \
        .withColumn("classificacao",
            when(col("indicador") == "Selic",
                when(col("valor_double") >= 0.12, "alta")
                .when(col("valor_double") >= 0.08, "moderada")
                .otherwise("baixa"))
            .when(col("indicador") == "IPCA",
                when(col("valor_double") >= 0.5, "alta")
                .when(col("valor_double") >= 0.2, "moderada")
                .otherwise("baixa"))
            .when(col("indicador") == "Dolar PTAX",
                when(col("valor_double") >= 6, "alto")
                .when(col("valor_double") >= 5, "moderado")
                .otherwise("baixo"))
            .otherwise("neutro")) \
        .withColumn("valor_arredondado", spark_round(col("valor_double"), 4)) \
        .withColumn("ano", year(col("data_parsed"))) \
        .withColumn("mes", month(col("data_parsed")))

    print(f"Total de registros transformados: {df_silver.count()}")

    df_silver.groupBy("indicador", "classificacao").count().show(truncate=False)

    df_silver.select(
        "codigo", "indicador", "data", "valor_arredondado",
        "classificacao", "ano", "mes"
    ).write \
        .mode("overwrite") \
        .partitionBy("ano", "mes") \
        .parquet(output_path)

    print(f"Salvo em: {output_path}")
    spark.stop()


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    transform(input_path, output_path)
