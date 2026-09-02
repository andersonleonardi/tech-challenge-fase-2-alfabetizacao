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

# # Bronze -> Silver
# 
# Objetivo:
# Validar e se necessário limpar as tabelas da Bronze, armazenando na Silver e enriquecer os dados

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação meta_alfabetizacao_brasil

# CELL ********************

df_meta_brasil = spark.read.parquet(
    "Files/bronze/meta_alfabetizacao_brasil/meta_alfabetizacao_brasil.parquet"
)

display(df_meta_brasil)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validando se o Schema foi preservado

# CELL ********************

df_meta_brasil.printSchema()

print(f"Total de registros: {df_meta_brasil.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validaões de qualidade:
# - ano não pode ser nulo
# - rede não pode ser nula
# - taxa_alfabetizacao deve ficar entre 0 e 100


# CELL ********************

df_meta_brasil_validado = (
    df_meta_brasil
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("rede").isNull(), "rede_nula")
         .when(~F.col("taxa_alfabetizacao").between(0, 100), "taxa_fora_intervalo")
    )
    .withColumn(
        "dq_valido",
        F.col("dq_motivo").isNull()
    )
)

display(df_meta_brasil_validado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Separando os registros validos dos que irão para quarentena

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = (
    df_meta_brasil_validado
    .filter(F.col("dq_valido") == True)
)

df_quarantine = (
    df_meta_brasil_validado
    .filter(F.col("dq_valido") == False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Salvando os validos na "tabela silver_meta_alfabetizacao_brasil"

# CELL ********************

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_meta_alfabetizacao_brasil")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Caso exista salva os que não passaram na validação na quarentena

# CELL ********************

if not df_quarantine.isEmpty():
    (
        df_quarantine.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_meta_alfabetizacao_brasil")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação meta_alfabetizacao_uf

# MARKDOWN ********************

# Validando se o Schema foi preservado

# CELL ********************

df_meta_uf = spark.read.parquet(
    "Files/bronze/meta_alfabetizacao_uf/meta_alfabetizacao_uf.parquet"
)

df_meta_uf.printSchema()
print(f"Total de registros: {df_meta_uf.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validaões de qualidade:
# - ano não pode ser nulo
# - sigla_uf não pode ser nulo
# - rede não pode ser nula
# - taxa_alfabetizacao deve ficar entre 0 e 100

# CELL ********************

df_meta_uf_validado = (
    df_meta_uf
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("sigla_uf").isNull(), "sigla_uf_nula")
         .when(F.col("rede").isNull(), "rede_nula")
         .when(~F.col("taxa_alfabetizacao").between(0, 100), "taxa_fora_intervalo")
    )
    .withColumn(
        "dq_valido",
        F.col("dq_motivo").isNull()
    )
)

display(df_meta_uf_validado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Separando e salvando os registros validos dos que irão para quarentena

# CELL ********************

df_silver_meta_uf = (
    df_meta_uf_validado
    .filter(F.col("dq_valido") == True)
    .drop("dq_valido", "dq_motivo")
)

df_quarantine_meta_uf = (
    df_meta_uf_validado
    .filter(F.col("dq_valido") == False)
)

(
    df_silver_meta_uf.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_meta_alfabetizacao_uf")
)

if not df_quarantine_meta_uf.isEmpty():
    (
        df_quarantine_meta_uf.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_meta_alfabetizacao_uf")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação meta_alfabetizacao_municipio.

# MARKDOWN ********************

# Validando se o Schema foi preservado

# CELL ********************

df_meta_municipio = spark.read.parquet(
    "Files/bronze/meta_alfabetizacao_municipio/meta_alfabetizacao_municipio.parquet"
)

df_meta_municipio.printSchema()
print(f"Total de registros: {df_meta_municipio.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Primeiras Validações de qualidade:
# - ano não pode ser nulo
# - id_municipio não pode ser nulo
# - rede não pode ser nula
# - taxa_alfabetizacao não ser nulo e deve ficar entre 0 e 100
# - percentual_participacao não ser nulo e deve ficar entre 0 e 100

# CELL ********************

df_meta_municipio_validado = (
    df_meta_municipio
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("id_municipio").isNull(), "id_municipio_nulo")
         .when(F.col("rede").isNull(), "rede_nula")
         .when(
             F.col("taxa_alfabetizacao").isNotNull() &
             ~F.col("taxa_alfabetizacao").between(0, 100),
             "taxa_fora_intervalo"
         )
         .when(
             F.col("percentual_participacao").isNotNull() &
             ~F.col("percentual_participacao").between(0, 100),
             "participacao_fora_intervalo"
         )
    )
    .withColumn(
        "dq_valido",
        F.col("dq_motivo").isNull()
    )
)

display(df_meta_uf_validado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Verificando a existencia de nulos

# CELL ********************

df_meta_municipio_validado.select(
    F.count(F.when(F.col("taxa_alfabetizacao").isNull(), 1)).alias("taxa_nula"),
    F.count(F.when(F.col("nivel_alfabetizacao").isNull(), 1)).alias("nivel_nulo"),
    F.count(F.when(F.col("percentual_participacao").isNull(), 1)).alias("participacao_nula"),
    F.count(F.when(F.col("meta_alfabetizacao_2024").isNull(), 1)).alias("meta_2024_nula")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_meta_municipio_validado.groupBy("ano").agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("taxa_alfabetizacao").isNull(), 1)).alias("taxa_nula"),
    F.count(F.when(F.col("nivel_alfabetizacao").isNull(), 1)).alias("nivel_nulo"),
    F.count(
        F.when(F.col("percentual_participacao").isNull(), 1)
    ).alias("participacao_nula"),
    F.count(
        F.when(F.col("meta_alfabetizacao_2024").isNull(), 1)
    ).alias("meta_2024_nula")
).orderBy("ano").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **OBS.:** Decisão de manter os nulos, pois parece revelar falta de informação para apurar, e não um erro, nesse caso a falta de informação pode ser uma informação relevante que justifique a falta de un indicador

# MARKDOWN ********************

# Separando e salvando os registros validos dos que irão para quarentena

# CELL ********************

df_silver_meta_municipio = (
    df_meta_municipio_validado
    .filter(F.col("dq_valido") == True)
    .drop("dq_valido", "dq_motivo")
)

df_quarantine_meta_municipio = (
    df_meta_municipio_validado
    .filter(F.col("dq_valido") == False)
)

(
    df_silver_meta_municipio.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_meta_alfabetizacao_municipio")
)

if not df_quarantine_meta_municipio.isEmpty():
    (
        df_quarantine_meta_municipio.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_meta_alfabetizacao_municipio")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Função para verificar duplicidade

# CELL ********************

def verificar_duplicidades(df, colunas_chave):
    return (
        df.groupBy(*colunas_chave)
          .count()
          .filter(F.col("count") > 1)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Função para resumo de qualidade

# CELL ********************

def resumo_qualidade(df, nome_tabela, colunas_chave):
    total = df.count()
    
    duplicados = (
        verificar_duplicidades(df, colunas_chave)
        .count()
    )
    
    print(f"Tabela: {nome_tabela}")
    print(f"Total de registros: {total}")
    print(f"Chave analisada: {', '.join(colunas_chave)}")
    print(f"Grupos duplicados: {duplicados}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Resumo de qualidade

# CELL ********************

resumo_qualidade(
    df_silver,
    "meta_alfabetizacao_brasil",
    ["ano", "rede"]
)

resumo_qualidade(
    df_silver_meta_uf,
    "meta_alfabetizacao_uf",
    ["ano", "sigla_uf", "rede"]
)

resumo_qualidade(
    df_silver_meta_municipio,
    "meta_alfabetizacao_municipio",
    ["ano", "id_municipio", "rede"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação uf

# MARKDOWN ********************

# Validando se o Schema foi preservado

# CELL ********************

df_uf = spark.read.parquet(
    "Files/bronze/uf/uf.parquet"
)

df_uf.printSchema()
print(f"Total de registros: {df_uf.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validaões de qualidade:
# - ano não pode ser nulo
# - sigla_uf não pode ser nulo
# - serie não pode ser nula
# - rede não pode ser nula
# - taxa_alfabetizacao não ser nula e deve ficar entre 0 e 100
# - proporcao_aluno_nivel_X deve ficar entre 0 e 100

# CELL ********************

colunas_proporcao = [
    f"proporcao_aluno_nivel_{i}" for i in range(9)
]

condicao_proporcao_invalida = F.lit(False)

for coluna in colunas_proporcao:
    condicao_proporcao_invalida = (
        condicao_proporcao_invalida |
        (
            F.col(coluna).isNotNull() &
            ~F.col(coluna).between(0, 100)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expressoes = [
    F.count(
        F.when(F.col(c).isNull(), 1)
    ).alias(f"{c}_nulos")
    for c in colunas_proporcao
]

display(df_uf.groupBy("ano").agg(*expressoes).orderBy("ano"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_uf_validado = (
    df_uf
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("sigla_uf").isNull(), "sigla_uf_nula")
         .when(F.col("serie").isNull(), "serie_nula")
         .when(F.col("rede").isNull(), "rede_nula")
         .when(
             F.col("taxa_alfabetizacao").isNotNull() &
             ~F.col("taxa_alfabetizacao").between(0, 100),
             "taxa_fora_intervalo"
         )
         .when(
             condicao_proporcao_invalida,
             "proporcao_nivel_fora_intervalo"
         )
    )
    .withColumn("dq_valido", F.col("dq_motivo").isNull())
)

display(df_uf_validado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Resumo de qualidade

# CELL ********************

resumo_qualidade(
    df_uf_validado,
    "uf",
    ["ano", "sigla_uf", "serie", "rede"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Separando e salvando os registros validos dos que irão para quarentena

# CELL ********************

df_silver_uf = (
    df_uf_validado
    .filter(F.col("dq_valido") == True)
    .drop("dq_valido", "dq_motivo")
)

df_quarantine_uf = (
    df_uf_validado
    .filter(F.col("dq_valido") == False)
)

(
    df_silver_uf.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_uf")
)

if not df_quarantine_uf.isEmpty():
    (
        df_quarantine_uf.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_uf")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação municipio

# CELL ********************

df_municipio = spark.read.parquet(
    "Files/bronze/municipio/municipio.parquet"
)

df_municipio.printSchema()
print(f"Total de registros: {df_municipio.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validaões de qualidade:
# - ano não pode ser nulo
# - id_municipio não pode ser nulo
# - serie não pode ser nula
# - rede não pode ser nula
# - taxa_alfabetizacao não ser nula deve ficar entre 0 e 100
# - proporcao_aluno_nivel_X deve ficar entre 0 e 100 (aproveitando funcao feita para uf)

# CELL ********************

df_municipio_validado = (
    df_municipio
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("id_municipio").isNull(), "id_municipio_nulo")
         .when(F.col("serie").isNull(), "serie_nula")
         .when(F.col("rede").isNull(), "rede_nula")
         .when(
             F.col("taxa_alfabetizacao").isNotNull() &
             ~F.col("taxa_alfabetizacao").between(0, 100),
             "taxa_fora_intervalo"
         )
         .when(
             condicao_proporcao_invalida,
             "proporcao_nivel_fora_intervalo"
         )
    )
    .withColumn(
        "dq_valido",
        F.col("dq_motivo").isNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validando nulos das proporções para decidir se vao pra quarentena

# CELL ********************

expressoes = [
    F.count(
        F.when(F.col(c).isNull(), 1)
    ).alias(f"{c}_nulos")
    for c in colunas_proporcao
]

display(df_municipio \
    .groupBy("ano") \
    .agg(*expressoes) \
    .orderBy("ano"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Resumo de qualidade

# CELL ********************

resumo_qualidade(
    df_municipio_validado,
    "municipio",
    ["ano", "id_municipio", "serie", "rede"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Separando e salvando os registros validos dos que irão para quarentena

# CELL ********************

df_silver_municipio = (
    df_municipio_validado
    .filter(F.col("dq_valido") == True)
    .drop("dq_valido", "dq_motivo")
)

df_quarantine_municipio = (
    df_municipio_validado
    .filter(F.col("dq_valido") == False)
)

(
    df_silver_municipio.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_municipio")
)

if not df_quarantine_municipio.isEmpty():
    (
        df_quarantine_municipio.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_municipio")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validação alunos (maior e mais complexa tabela)

# CELL ********************

df_alunos = spark.read.parquet(
    "Files/bronze/alunos/alunos.parquet"
)

df_alunos.printSchema()
print(f"Total de registros: {df_alunos.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Verificação de qualidade
# 
# Verificando o preenchimento dos campos que considero os principais

# CELL ********************

df_alunos.groupBy(
    "ano",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado"
).agg(
    F.count("*").alias("quantidade"),
    F.count(F.when(F.col("proficiencia").isNull(), 1)).alias("proficiencia_nula"),
    F.count(F.when(F.col("peso_aluno").isNull(), 1)).alias("peso_nulo")
).orderBy(
    "ano",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado"
).show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos.select(
    F.count("*").alias("total"),

    F.count(
        F.when(F.col("id_aluno").isNull(), 1)
    ).alias("id_aluno_nulo"),

    F.count(
        F.when(F.col("id_municipio").isNull(), 1)
    ).alias("id_municipio_nulo"),

    F.count(
        F.when(F.col("id_escola").isNull(), 1)
    ).alias("id_escola_nulo"),

    F.count(
        F.when(F.col("proficiencia").isNull(), 1)
    ).alias("proficiencia_nula"),

    F.count(
        F.when(F.col("peso_aluno").isNull(), 1)
    ).alias("peso_nulo")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validando a regra do enunciado definindo 743 como meta para considerar alfabetizado

# CELL ********************

df_alunos.select(
    F.count(
        F.when(
            (F.col("proficiencia").isNotNull()) &
            (F.col("proficiencia") >= 743) &
            (F.col("alfabetizado") != 1),
            1
        )
    ).alias("acima_743_nao_alfabetizado"),

    F.count(
        F.when(
            (F.col("proficiencia").isNotNull()) &
            (F.col("proficiencia") < 743) &
            (F.col("alfabetizado") != 0),
            1
        )
    ).alias("abaixo_743_alfabetizado")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validando id_aluno como chave unica ou composta

# CELL ********************

df_alunos.select(
    F.count("*").alias("registros"),
    F.countDistinct("id_aluno").alias("id_aluno_distintos")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos.groupBy(
    "ano",
    "id_aluno"
).count().filter(
    F.col("count") > 1
).agg(
    F.count("*").alias("chaves_duplicadas"),
    F.max("count").alias("max_registros_mesma_chave")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validaões de qualidade:
# - ano não pode ser nulo
# - id_aluno não pode ser nulo
# - id_municipio não pode ser nula
# - id_escola não pode ser nula
# - presenca não pode ser nula
# - preenchimento_caderno não pode ser nula
# - alfabetizado não pode ser nula
# - presenca verdadeiro ou faoso (0,1)
# - preenchimento_caderno verdadeiro ou faoso (0,1)
# - alfabetizado verdadeiro ou faoso (0,1)
# - para ser considerado alfabetizado proficiencia nao pode ser nulo e ser maior ou igual a 743


# CELL ********************

df_alunos_validado = (
    df_alunos
    .withColumn(
        "dq_motivo",
        F.when(F.col("ano").isNull(), "ano_nulo")
         .when(F.col("id_aluno").isNull(), "id_aluno_nulo")
         .when(F.col("id_municipio").isNull(), "id_municipio_nulo")
         .when(F.col("id_escola").isNull(), "id_escola_nulo")
         .when(F.col("presenca").isNull(), "presenca_nula")
         .when(F.col("preenchimento_caderno").isNull(), "preenchimento_nulo")
         .when(F.col("alfabetizado").isNull(), "alfabetizado_nulo")

         .when(~F.col("presenca").isin(0, 1), "presenca_invalida")
         .when(
             ~F.col("preenchimento_caderno").isin(0, 1),
             "preenchimento_invalido"
         )
         .when(
             ~F.col("alfabetizado").isin(0, 1),
             "alfabetizado_invalido"
         )

         .when(
             (F.col("proficiencia").isNotNull()) &
             (F.col("proficiencia") >= 743) &
             (F.col("alfabetizado") != 1),
             "inconsistencia_regra_743"
         )
         .when(
             (F.col("proficiencia").isNotNull()) &
             (F.col("proficiencia") < 743) &
             (F.col("alfabetizado") != 0),
             "inconsistencia_regra_743"
         )
    )
    .withColumn(
        "dq_valido",
        F.col("dq_motivo").isNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Resumo qualidade

# CELL ********************

resumo_qualidade(
    df_alunos_validado,
    "alunos",
    ["ano", "id_aluno"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alunos_validado.groupBy(
    "dq_valido",
    "dq_motivo"
).count().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Separando e salvando os registros validos dos que irão para quarentena

# CELL ********************

df_silver_aluno = (
    df_alunos_validado
    .filter(F.col("dq_valido") == True)
    .drop("dq_valido", "dq_motivo")
)

df_quarantine_aluno = (
    df_alunos_validado
    .filter(F.col("dq_valido") == False)
)

(
    df_silver_aluno.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_aluno")
)

if not df_quarantine_aluno.isEmpty():
    (
        df_quarantine_aluno.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("quarantine_aluno")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Enriquecer os dados

# MARKDOWN ********************

# Dicionario para referencia

# CELL ********************

df_dicionario = spark.read.parquet(
    "Files/bronze/dicionario/dicionario.parquet"
)

display(df_dicionario)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dicionario.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dicionario.filter(
    F.col("nome_coluna") == "rede"
).orderBy(
    "id_tabela",
    "chave"
).show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# funcao para retornar o dominio desejado da tabela dicionario

# CELL ********************

def obter_lookup_dicionario(df_dicionario, tabela, coluna, nome_saida):
    return (
        df_dicionario
        .filter(
            (F.col("id_tabela") == tabela) &
            (F.col("nome_coluna") == coluna)
        )
        .select(
            F.col("chave").cast("int").alias(coluna),
            F.col("valor").alias(nome_saida)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Analisando tabela externa "diretorio_municipio" para trazer o nome do estado e do municipio

# CELL ********************

df_diretorio_municipio = spark.read.parquet(
    "Files/bronze/diretorio_municipio/diretorio_municipio.parquet"
)

df_diretorio_municipio.printSchema()

print(
    f"Total de registros: {df_diretorio_municipio.count():,}"
)

display(df_diretorio_municipio.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_diretorio_municipio.select(
    F.count("*").alias("registros"),
    F.countDistinct("id_municipio").alias("municipios_distintos")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Ciar uma visao enxuta na Silver para servir de dominio e pode também ser aproveitada como informação

# CELL ********************

df_silver_dim_municipio = (
    df_diretorio_municipio
    .select(
        "id_municipio",
        F.col("nome").alias("nome_municipio"),
        "sigla_uf",
        "nome_uf",
        "nome_regiao"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_dim_municipio.select(
    F.count("*").alias("total"),
    F.count(F.when(F.col("id_municipio").isNull(), 1)).alias("id_municipio_nulo"),
    F.count(F.when(F.col("nome_municipio").isNull(), 1)).alias("nome_municipio_nulo"),
    F.count(F.when(F.col("sigla_uf").isNull(), 1)).alias("sigla_uf_nula")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_dim_municipio.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_dim_municipio")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Municipio

# MARKDOWN ********************

# Adicionando descricao para o campo rede

# CELL ********************

df_dict_rede_municipio = obter_lookup_dicionario(
    df_dicionario,
    "municipio",
    "rede",
    "rede_descricao"
)

display(df_dict_rede_municipio)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_municipio = (
    spark.table("silver_municipio")
    .drop("rede_descricao")
    .join(
        df_dict_rede_municipio,
        on="rede",
        how="left"
    )
)

df_silver_municipio.select(
    "rede",
    "rede_descricao"
).distinct().orderBy("rede").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_municipio.filter(
    F.col("rede_descricao").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_municipio")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Enriquecer com a dimensão de municipio (nome, estado e regiao)

# CELL ********************

df_silver_municipio = (
    spark.table("silver_municipio")
    .drop(
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "nome_regiao"
    )
    .join(
        df_silver_dim_municipio,
        on="id_municipio",
        how="left"
    )
)

display(df_silver_municipio.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_municipio.filter(
    F.col("nome_municipio").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_municipio")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## uf

# MARKDOWN ********************

# Adicionando descricao para o campo rede

# CELL ********************

df_dict_rede_uf = obter_lookup_dicionario(
    df_dicionario,
    "uf",
    "rede",
    "rede_descricao"
)

df_silver_uf = (
    spark.table("silver_uf")
    .drop("rede_descricao")
    .join(
        df_dict_rede_uf,
        on="rede",
        how="left"
    )
)

df_silver_uf.select(
    "rede",
    "rede_descricao"
).distinct().orderBy("rede").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_uf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_uf")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Enriquecer a partir da dimensão de municipio (nome e regiao)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_uf = (
    spark.table("silver_dim_municipio")
    .select(
        "sigla_uf",
        "nome_uf",
        "nome_regiao"
    )
    .distinct()
)

df_dim_uf.orderBy("sigla_uf").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_uf.select(
    F.count("*").alias("registros"),
    F.countDistinct("sigla_uf").alias("ufs_distintas")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_uf = (
    spark.table("silver_uf")
    .drop(
        "nome_uf",
        "nome_regiao"
    )
    .join(
        df_dim_uf,
        on="sigla_uf",
        how="left"
    )
)

display(df_silver_uf.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_uf.filter(
    F.col("nome_uf").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_uf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_uf")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Alunos

# MARKDOWN ********************

# Adicionando descricao para os campos:
# - rede + rede_descricao
# - serie + serie_descricao
# - presenca + presenca_descricao
# - preenchimento_caderno + preenchimento_caderno_descricao
# - alfabetizado + alfabetizado_descricao

# CELL ********************

df_dict_rede_alunos = obter_lookup_dicionario(
    df_dicionario,
    "alunos",
    "rede",
    "rede_descricao"
)

df_dict_serie_alunos = obter_lookup_dicionario(
    df_dicionario,
    "alunos",
    "serie",
    "serie_descricao"
)

df_dict_presenca_alunos = obter_lookup_dicionario(
    df_dicionario,
    "alunos",
    "presenca",
    "presenca_descricao"
)

df_dict_preenchimento_alunos = obter_lookup_dicionario(
    df_dicionario,
    "alunos",
    "preenchimento_caderno",
    "preenchimento_caderno_descricao"
)

df_dict_alfabetizado_alunos = obter_lookup_dicionario(
    df_dicionario,
    "alunos",
    "alfabetizado",
    "alfabetizado_descricao"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_alunos = (
    spark.table("silver_aluno")

    .drop(
        "rede_descricao",
        "serie_descricao",
        "presenca_descricao",
        "preenchimento_caderno_descricao",
        "alfabetizado_descricao"
    )

    .join(df_dict_rede_alunos, on="rede", how="left")
    .join(df_dict_serie_alunos, on="serie", how="left")
    .join(df_dict_presenca_alunos, on="presenca", how="left")
    .join(
        df_dict_preenchimento_alunos,
        on="preenchimento_caderno",
        how="left"
    )
    .join(
        df_dict_alfabetizado_alunos,
        on="alfabetizado",
        how="left"
    )
)

display(df_silver_alunos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_alunos.select(
    F.count("*").alias("total"),

    F.count(
        F.when(F.col("rede_descricao").isNull(), 1)
    ).alias("rede_sem_descricao"),

    F.count(
        F.when(F.col("serie_descricao").isNull(), 1)
    ).alias("serie_sem_descricao"),

    F.count(
        F.when(F.col("presenca_descricao").isNull(), 1)
    ).alias("presenca_sem_descricao"),

    F.count(
        F.when(F.col("preenchimento_caderno_descricao").isNull(), 1)
    ).alias("preenchimento_sem_descricao"),

    F.count(
        F.when(F.col("alfabetizado_descricao").isNull(), 1)
    ).alias("alfabetizado_sem_descricao")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_alunos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_aluno")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Metas Municipais

# MARKDOWN ********************

# As metas estao registradas em linha, transformar em colunas para facilitar relacionamento

# CELL ********************

df_meta_municipio = spark.table(
    "silver_meta_alfabetizacao_municipio"
)

colunas_meta = [
    f"meta_alfabetizacao_{ano}"
    for ano in range(2024, 2031)
]

df_meta_trajetoria = (
    df_meta_municipio
    .select(
        "id_municipio",
        *colunas_meta
    )
    .distinct()
)

display(df_meta_trajetoria.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_meta_trajetoria.select(
    F.count("*").alias("registros"),
    F.countDistinct("id_municipio").alias("municipios")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_meta_municipio_long = (
    df_meta_trajetoria
    .unpivot(
        ids=["id_municipio"],
        values=colunas_meta,
        variableColumnName="meta_origem",
        valueColumnName="meta_alfabetizacao"
    )
    .withColumn(
        "ano_meta",
        F.regexp_extract(
            F.col("meta_origem"),
            r"(\d{4})$",
            1
        ).cast("int")
    )
    .select(
        "id_municipio",
        "ano_meta",
        "meta_alfabetizacao"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_meta_municipio_long.groupBy(
    "ano_meta"
).agg(
    F.count("*").alias("registros"),
    F.countDistinct("id_municipio").alias("municipios"),
    F.count(
        F.when(F.col("meta_alfabetizacao").isNull(), 1)
    ).alias("metas_nulas")
).orderBy("ano_meta").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_meta_municipio_long.groupBy(
    "id_municipio",
    "ano_meta"
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
    df_silver_meta_municipio_long.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_meta_municipio_long")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
