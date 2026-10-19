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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df_alunos = spark.read.parquet(
    "Files/bronze/alunos/alunos.parquet"
)

display(df_alunos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_alunos.limit(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos_2023 = df_alunos.filter(df_alunos.ano == 2023)

df_alunos_2023.select(
    "id_escola",
    "id_municipio"
).distinct().show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos_2023.select("id_escola").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

# Ajustar o caminho conforme o Lakehouse
caminho = "Files/bronze/censo_escolar/2023/microdados_ed_basica_2023.csv"


df_censo = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", True)
    .csv(caminho)
)

# Apenas escolas em funcionamento
df_escolas = df_censo.filter(
    F.col("TP_SITUACAO_FUNCIONAMENTO") == 1
)

# Primeira agregação municipal
df_municipios = (
    df_escolas
    .groupBy("NU_ANO_CENSO", "CO_MUNICIPIO")
    .agg(
        F.countDistinct("CO_ENTIDADE").alias("qtd_escolas"),
        F.sum(F.when(F.col("TP_LOCALIZACAO") == 2, 1)
              .otherwise(0)).alias("qtd_escolas_rurais"),
        F.sum(F.when(F.col("IN_INTERNET") == 1, 1)
              .otherwise(0)).alias("qtd_escolas_internet"),
        F.sum("QT_MAT_FUND_AI").alias("matriculas_anos_iniciais")
    )
)

display(df_municipios.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


print("Escolas em funcionamento:", df_escolas.count())

print("Municípios agregados:", df_municipios.count())

print("Chaves duplicadas:",
    df_municipios.groupBy(
        "NU_ANO_CENSO", "CO_MUNICIPIO"
    ).count().filter("count > 1").count()
)

print("Anos encontrados:")
df_municipios.groupBy("NU_ANO_CENSO").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze_alunos = spark.read.parquet("Files/bronze/alunos/alunos.parquet")

# Municípios distintos da base de alunos em 2023
df_municipios_alunos = (
    df_bronze_alunos.filter(F.col("ano") == 2023)
      .select(
          F.col("id_municipio").cast("string").alias("id_municipio")
      )
      .distinct()
)

# Municípios agregados do Censo Escolar
df_municipios_censo = (
    df_municipios
    .filter(F.col("NU_ANO_CENSO") == 2023)
    .select(
        F.col("CO_MUNICIPIO").cast("string").alias("id_municipio")
    )
    .distinct()
)

# Municípios sem correspondência
df_sem_match = df_municipios_alunos.join(
    df_municipios_censo,
    on="id_municipio",
    how="left_anti"
)

print("Municípios na base de alunos:",
      df_municipios_alunos.count())

print("Municípios sem correspondência:",
      df_sem_match.count())

display(df_sem_match.limit(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


colunas = [
    "IN_INTERNET",
    "IN_BIBLIOTECA",
    "IN_SALA_LEITURA",
    "QT_MAT_FUND_AI",
    "QT_DOC_FUND_AI",
    "QT_TUR_FUND_AI",
    "QT_MAT_FUND_AI_INT"
]

for coluna in colunas:
    print(f"{coluna}:")
    df_escolas.groupBy(coluna).count().orderBy(
        F.desc("count")
    ).show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

# Escolas em funcionamento com matrículas nos anos iniciais
df_escolas_ai = df_escolas.filter(
    F.col("QT_MAT_FUND_AI") > 0
)

# Biblioteca OU sala de leitura
df_escolas_ai = df_escolas_ai.withColumn(
    "possui_espaco_leitura",
    F.when(
        (F.col("IN_BIBLIOTECA") == 1) |
        (F.col("IN_SALA_LEITURA") == 1),
        1
    ).otherwise(0)
)

# Agregação municipal
df_silver_censo_2023 = (
    df_escolas_ai
    .groupBy("NU_ANO_CENSO", "CO_MUNICIPIO")
    .agg(
        F.countDistinct("CO_ENTIDADE").alias("qtd_escolas_ai"),
        F.sum(F.when(F.col("TP_LOCALIZACAO") == 2, 1)
              .otherwise(0)).alias("qtd_escolas_rurais"),
        F.sum("IN_INTERNET").alias("qtd_escolas_internet"),
        F.sum("possui_espaco_leitura").alias("qtd_escolas_leitura"),
        F.sum("QT_MAT_FUND_AI").alias("qtd_matriculas_ai"),
        F.sum("QT_DOC_FUND_AI").alias("qtd_docentes_ai"),
        F.sum("QT_TUR_FUND_AI").alias("qtd_turmas_ai"),
        F.sum("QT_MAT_FUND_AI_INT").alias("qtd_matriculas_integral_ai")
    )
    .withColumn(
        "pct_escolas_rurais",
        F.round(F.col("qtd_escolas_rurais") /
                F.col("qtd_escolas_ai") * 100, 2)
    )
    .withColumn(
        "pct_escolas_internet",
        F.round(F.col("qtd_escolas_internet") /
                F.col("qtd_escolas_ai") * 100, 2)
    )
    .withColumn(
        "pct_escolas_leitura",
        F.round(F.col("qtd_escolas_leitura") /
                F.col("qtd_escolas_ai") * 100, 2)
    )
    .withColumn(
        "alunos_por_docente",
        F.when(
            F.col("qtd_docentes_ai") > 0,
            F.round(
                F.col("qtd_matriculas_ai") /
                F.col("qtd_docentes_ai"), 2
            )
        )
    )
    .withColumn(
        "alunos_por_turma",
        F.when(
            F.col("qtd_turmas_ai") > 0,
            F.round(
                F.col("qtd_matriculas_ai") /
                F.col("qtd_turmas_ai"), 2
            )
        )
    )
    .withColumn(
        "pct_matriculas_integral",
        F.round(
            F.col("qtd_matriculas_integral_ai") /
            F.col("qtd_matriculas_ai") * 100, 2
        )
    )
)

display(df_silver_censo_2023.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


print("Escolas com anos iniciais:", df_escolas_ai.count())
print("Municípios agregados:", df_silver_censo_2023.count())

print("Municípios sem correspondência na base de alunos:",
    df_municipios_alunos.join(
        df_silver_censo_2023.select(
            F.col("CO_MUNICIPIO").cast("string")
             .alias("id_municipio")
        ),
        on="id_municipio",
        how="left_anti"
    ).count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


indicadores = [
    "pct_escolas_rurais",
    "pct_escolas_internet",
    "pct_escolas_leitura",
    "pct_matriculas_integral"
]

for coluna in indicadores:
    invalidos = df_silver_censo_2023.filter(
        (F.col(coluna) < 0) |
        (F.col(coluna) > 100) |
        F.col(coluna).isNull()
    ).count()
    print(f"{coluna}: {invalidos} inválidos")

print(
    "Municípios com docentes zerados:",
    df_silver_censo_2023.filter(
        F.col("qtd_docentes_ai") == 0
    ).count()
)

print(
    "Municípios com turmas zeradas:",
    df_silver_censo_2023.filter(
        F.col("qtd_turmas_ai") == 0
    ).count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


(
    df_silver_censo_2023
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_censo_escolar_municipal_2023")
)

print("Silver Censo Escolar 2023 salva com sucesso!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_validacao = spark.table(
    "silver_censo_escolar_municipal_2023"
)

print("Registros salvos:", df_validacao.count())
display(df_validacao.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
