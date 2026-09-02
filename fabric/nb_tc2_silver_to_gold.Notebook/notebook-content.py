# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "017d68f6-197f-4bd8-a06e-49a21a2c5d18",
# META       "default_lakehouse_name": "lh_tc2_alfabetizacao",
# META       "default_lakehouse_workspace_id": "b3909c63-42fa-4520-87ed-683ada991154",
# META       "known_lakehouses": [
# META         {
# META           "id": "017d68f6-197f-4bd8-a06e-49a21a2c5d18"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver → Gold
# 
# Objetivo:
# Construir as tabelas analíticas da camada Gold a partir das entidades
# curadas da camada Silver.
# 
# Saídas previstas:
# - Indicador de alfabetização por município
# - Comparação entre meta e resultado
# - Evolução temporal da alfabetização
# 
# Antes da materialização das tabelas Gold, serão analisados:
# - granularidade das entidades;
# - domínios de rede;
# - períodos disponíveis;
# - compatibilidade entre resultados e metas.

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validar cobertura entre resultado municipal e meta municipal

# CELL ********************

df_resultado_municipal = (
    spark.table("silver_municipio")
    .filter(F.col("rede_descricao") == "Municipal")
)

df_meta_municipal = (
    spark.table("silver_meta_alfabetizacao_municipio")
    .filter(F.col("rede") == "Municipal")
)

print("RESULTADOS MUNICIPAIS POR ANO")
df_resultado_municipal.groupBy("ano").count().orderBy("ano").show()

print("METAS MUNICIPAIS POR ANO")
df_meta_municipal.groupBy("ano").count().orderBy("ano").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cobertura = (
    df_resultado_municipal.alias("r")
    .join(
        df_meta_municipal.alias("m"),
        on=["ano", "id_municipio"],
        how="full"
    )
    .select(
        "ano",
        "id_municipio",
        F.when(
            F.col("r.id_municipio").isNotNull() &
            F.col("m.id_municipio").isNotNull(),
            "resultado_e_meta"
        )
        .when(
            F.col("r.id_municipio").isNotNull(),
            "somente_resultado"
        )
        .otherwise("somente_meta")
        .alias("situacao")
    )
)

df_cobertura.groupBy(
    "ano",
    "situacao"
).count().orderBy(
    "ano",
    "situacao"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Como meta e resultado dos municipios nao sao 1x1. Vamos tratar

# CELL ********************

df_gold_indicador_municipio = (
    spark.table("silver_municipio")
    .filter(F.col("rede_descricao") == "Municipal")
    .select(
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "nome_regiao",
        "serie",
        "rede",
        "rede_descricao",
        "taxa_alfabetizacao",
        "media_portugues"
    )
)

display(df_gold_indicador_municipio.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_indicador_municipio.groupBy("ano").agg(
    F.count("*").alias("registros"),
    F.countDistinct("id_municipio").alias("municipios")
).orderBy("ano").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_indicador_municipio.groupBy(
    "ano",
    "id_municipio"
).count().filter(
    F.col("count") > 1
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Temos o indicador de resultado dos municipios

# CELL ********************

(
    df_gold_indicador_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_indicador_municipio")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Vamos pegar agora as metas dos municipios para que possamos relacionar com o resultado

# CELL ********************

df_resultado = (
    spark.table("silver_municipio")
    .filter(F.col("rede_descricao") == "Municipal")
)

df_meta = spark.table("silver_meta_municipio_long")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_meta_vs_resultado = (
    df_resultado.alias("r")
    .join(
        df_meta.alias("m"),
        (F.col("r.id_municipio") == F.col("m.id_municipio")) &
        (F.col("r.ano") == F.col("m.ano_meta")),
        how="left"
    )
    .select(
        F.col("r.ano"),
        F.col("r.id_municipio"),
        F.col("r.nome_municipio"),
        F.col("r.sigla_uf"),
        F.col("r.nome_uf"),
        F.col("r.nome_regiao"),
        F.col("r.taxa_alfabetizacao").alias("resultado_alfabetizacao"),
        F.col("m.meta_alfabetizacao"),
        (
            F.col("r.taxa_alfabetizacao") -
            F.col("m.meta_alfabetizacao")
        ).alias("diferenca_meta")
    )
)

display(df_gold_meta_vs_resultado.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_meta_vs_resultado = (
    df_gold_meta_vs_resultado
    .withColumn(
        "situacao_meta",
        F.when(
            F.col("meta_alfabetizacao").isNull(),
            "Sem meta"
        )
        .when(
            F.col("resultado_alfabetizacao") >=
            F.col("meta_alfabetizacao"),
            "Meta atingida"
        )
        .otherwise("Meta não atingida")
    )
)

display(df_gold_meta_vs_resultado.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_meta_vs_resultado.groupBy(
    "ano",
    "situacao_meta"
).count().orderBy(
    "ano",
    "situacao_meta"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_meta_vs_resultado.groupBy(
    "ano",
    "id_municipio"
).count().filter(
    F.col("count") > 1
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_gold_meta_vs_resultado.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_meta_vs_resultado")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Agora vamos calcular a evolucao do municipio entre os anos. Aproveitar a tabela "gold_indicador_municipio"

# CELL ********************

df_indicador = spark.table("gold_indicador_municipio")

df_evolucao = (
    df_indicador
    .groupBy(
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "nome_regiao"
    )
    .pivot("ano", [2023, 2024])
    .agg(
        F.first("taxa_alfabetizacao")
    )
    .withColumnRenamed("2023", "taxa_alfabetizacao_2023")
    .withColumnRenamed("2024", "taxa_alfabetizacao_2024")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_evolucao_alfabetizacao = (
    df_evolucao
    .withColumn(
        "variacao_pp",
        F.col("taxa_alfabetizacao_2024") -
        F.col("taxa_alfabetizacao_2023")
    )
    .withColumn(
        "situacao_evolucao",
        F.when(
            F.col("taxa_alfabetizacao_2023").isNull() |
            F.col("taxa_alfabetizacao_2024").isNull(),
            "Sem comparação"
        )
        .when(
            F.col("variacao_pp") > 0,
            "Melhora"
        )
        .when(
            F.col("variacao_pp") < 0,
            "Piora"
        )
        .otherwise("Estável")
    )
)

display(df_gold_evolucao_alfabetizacao.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_evolucao_alfabetizacao.groupBy(
    "situacao_evolucao"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_evolucao_alfabetizacao.select(
    F.count("*").alias("registros"),
    F.countDistinct("id_municipio").alias("municipios"),
    F.count(
        F.when(F.col("taxa_alfabetizacao_2023").isNull(), 1)
    ).alias("sem_2023"),
    F.count(
        F.when(F.col("taxa_alfabetizacao_2024").isNull(), 1)
    ).alias("sem_2024")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_gold_evolucao_alfabetizacao
    .orderBy(F.desc("variacao_pp"))
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_gold_evolucao_alfabetizacao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_evolucao_alfabetizacao")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
