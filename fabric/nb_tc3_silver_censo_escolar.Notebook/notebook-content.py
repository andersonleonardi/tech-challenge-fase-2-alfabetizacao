# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffd90b59-5acd-4630-94dc-e5284df5f3cc",
# META       "default_lakehouse_name": "lh_tc3_alfabetizacao",
# META       "default_lakehouse_workspace_id": "b3909c63-42fa-4520-87ed-683ada991154",
# META       "known_lakehouses": [
# META         {
# META           "id": "ffd90b59-5acd-4630-94dc-e5284df5f3cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# Tech Challenge 3 | Silver - Censo Escolar
# Leitura inicial dos microdados de 2022

from pyspark.sql import functions as F

ano = 2022

caminho = (
    f"Files/bronze/censo_escolar_{ano}/"
    f"{ano}_microdados_ed_basica.csv"
)

df_bronze = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", False)
    .csv(caminho)
)

print(f"Ano: {ano}")
print(f"Registros: {df_bronze.count():,}")
print(f"Colunas: {len(df_bronze.columns)}")

df_bronze.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Seleção das escolas elegíveis - 2022

df_escolas_2022 = (
    df_bronze
    .withColumn(
        "TP_SITUACAO_FUNCIONAMENTO",
        F.col("TP_SITUACAO_FUNCIONAMENTO").cast("int")
    )
    .withColumn(
        "QT_MAT_FUND_AI",
        F.col("QT_MAT_FUND_AI").cast("int")
    )
    .filter(
        (F.col("TP_SITUACAO_FUNCIONAMENTO") == 1) &
        (F.col("QT_MAT_FUND_AI") > 0)
    )
)

print(f"Escolas originais: {df_bronze.count():,}")
print(f"Escolas selecionadas: {df_escolas_2022.count():,}")
print(
    "Municípios representados:",
    df_escolas_2022.select("CO_MUNICIPIO").distinct().count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Validação da granularidade escola/ano

total_registros = df_escolas_2022.count()

escolas_distintas = (
    df_escolas_2022
    .select("CO_ENTIDADE")
    .distinct()
    .count()
)

chaves_nulas = df_escolas_2022.filter(
    F.col("CO_ENTIDADE").isNull() |
    (F.trim(F.col("CO_ENTIDADE")) == "")
).count()

print(f"Registros: {total_registros:,}")
print(f"Escolas distintas: {escolas_distintas:,}")
print(f"Chaves nulas/vazias: {chaves_nulas:,}")
print(
    "Unicidade:",
    "VALIDADA"
    if total_registros == escolas_distintas and chaves_nulas == 0
    else "REVISAR"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Seleção e tipagem - 2022

colunas_texto = [
    "CO_ENTIDADE",
    "CO_MUNICIPIO"
]

colunas_inteiras = [
    "NU_ANO_CENSO",
    "TP_SITUACAO_FUNCIONAMENTO",
    "TP_LOCALIZACAO",
    "IN_INTERNET",
    "IN_BIBLIOTECA",
    "IN_SALA_LEITURA",
    "QT_MAT_FUND_AI",
    "QT_MAT_FUND_AI_INT",
    "QT_DOC_FUND_AI",
    "QT_TUR_FUND_AI"
]

df_silver_2022 = df_escolas_2022.select(
    *[
        F.trim(F.col(c)).alias(c)
        for c in colunas_texto
    ],
    *[
        F.col(c).cast("int").alias(c)
        for c in colunas_inteiras
    ]
)

print(f"Registros: {df_silver_2022.count():,}")
print(f"Colunas: {len(df_silver_2022.columns)}")

df_silver_2022.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Validação de valores nulos - 2022

resultado_nulos = df_silver_2022.select([
    F.sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in df_silver_2022.columns
])

display(resultado_nulos)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Validação de consistência - 2022

validacoes = {
    "Ano diferente de 2022":
        F.col("NU_ANO_CENSO") != 2022,

    "Escola fora de funcionamento":
        F.col("TP_SITUACAO_FUNCIONAMENTO") != 1,

    "Matrículas não positivas":
        F.col("QT_MAT_FUND_AI") <= 0,

    "Matrículas integrais negativas":
        F.col("QT_MAT_FUND_AI_INT") < 0,

    "Matrículas integrais acima do total":
        F.col("QT_MAT_FUND_AI_INT") > F.col("QT_MAT_FUND_AI"),

    "Docentes negativos":
        F.col("QT_DOC_FUND_AI") < 0,

    "Turmas negativas":
        F.col("QT_TUR_FUND_AI") < 0,

    "Localização inválida":
        ~F.col("TP_LOCALIZACAO").isin(1, 2),

    "Indicadores binários inválidos":
        (
            ~F.col("IN_INTERNET").isin(0, 1) |
            ~F.col("IN_BIBLIOTECA").isin(0, 1) |
            ~F.col("IN_SALA_LEITURA").isin(0, 1)
        )
}

resultado = df_silver_2022.agg(
    *[
        F.sum(
            F.when(condicao, 1).otherwise(0)
        ).alias(nome)
        for nome, condicao in validacoes.items()
    ]
)

display(resultado)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Persistência em Delta - 2022

nome_tabela = "silver_censo_escolar_2022"

(
    df_silver_2022.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(nome_tabela)
)

print(f"Tabela criada: {nome_tabela}")
print(f"Registros: {spark.table(nome_tabela).count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Validação da tabela Delta - 2022

df_validacao = spark.table("silver_censo_escolar_2022")

total = df_validacao.count()
escolas = df_validacao.select("CO_ENTIDADE").distinct().count()
municipios = df_validacao.select("CO_MUNICIPIO").distinct().count()
colunas = len(df_validacao.columns)

print(f"Registros: {total:,}")
print(f"Escolas distintas: {escolas:,}")
print(f"Municípios: {municipios:,}")
print(f"Colunas: {colunas}")

assert total == 105360, "Quantidade de registros divergente"
assert escolas == total, "Existem escolas duplicadas"
assert municipios == 5570, "Quantidade de municípios divergente"
assert colunas == 12, "Quantidade de colunas divergente"

print("Validação da Silver 2022: APROVADA")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Verificação do esquema de 2023

ano = 2023

caminho = (
    f"Files/bronze/censo_escolar_{ano}/"
    f"{ano}_microdados_ed_basica.csv"
)

df_bronze_2023 = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", False)
    .csv(caminho)
)

colunas_necessarias = (
    colunas_texto + colunas_inteiras
)

colunas_ausentes = [
    c for c in colunas_necessarias
    if c not in df_bronze_2023.columns
]

print(f"Registros originais: {df_bronze_2023.count():,}")
print(f"Colunas originais: {len(df_bronze_2023.columns)}")
print(f"Colunas necessárias ausentes: {colunas_ausentes}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Tratamento do Censo Escolar 2023

df_silver_2023 = (
    df_bronze_2023
    .filter(
        (F.col("TP_SITUACAO_FUNCIONAMENTO").cast("int") == 1) &
        (F.col("QT_MAT_FUND_AI").cast("int") > 0)
    )
    .select(
        *[
            F.trim(F.col(c)).alias(c)
            for c in colunas_texto
        ],
        *[
            F.col(c).cast("int").alias(c)
            for c in colunas_inteiras
        ]
    )
)

# Validações iniciais
total = df_silver_2023.count()
escolas = df_silver_2023.select("CO_ENTIDADE").distinct().count()
municipios = df_silver_2023.select("CO_MUNICIPIO").distinct().count()

print(f"Registros selecionados: {total:,}")
print(f"Escolas distintas: {escolas:,}")
print(f"Municípios: {municipios:,}")
print(f"Colunas: {len(df_silver_2023.columns)}")

assert total == escolas, "Existem escolas duplicadas"
assert municipios == 5570, "Quantidade de municípios divergente"
assert len(df_silver_2023.columns) == 12, "Esquema divergente"

print("Validação inicial 2023: APROVADA")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Validação de qualidade - 2023

# Atualizar a regra de validação do ano
validacoes["Ano diferente de 2022"] = (
    F.col("NU_ANO_CENSO") != 2023
)

# Remover o nome antigo e renomear a validação
validacoes["Ano diferente de 2023"] = validacoes.pop(
    "Ano diferente de 2022"
)

# Contagem de nulos
nulos = df_silver_2023.select([
    F.sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in df_silver_2023.columns
])

# Verificações de consistência
inconsistencias = df_silver_2023.agg(
    *[
        F.sum(
            F.when(condicao, 1).otherwise(0)
        ).alias(nome)
        for nome, condicao in validacoes.items()
    ]
)

print("Valores nulos:")
display(nulos)

print("Inconsistências:")
display(inconsistencias)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver | Persistência em Delta - 2023

nome_tabela = "silver_censo_escolar_2023"

(
    df_silver_2023.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(nome_tabela)
)

# Validação após persistência
df_validacao = spark.table(nome_tabela)

total = df_validacao.count()
escolas = df_validacao.select("CO_ENTIDADE").distinct().count()
municipios = df_validacao.select("CO_MUNICIPIO").distinct().count()
colunas = len(df_validacao.columns)

assert total == 103785, "Quantidade de registros divergente"
assert escolas == total, "Existem escolas duplicadas"
assert municipios == 5570, "Quantidade de municípios divergente"
assert colunas == 12, "Quantidade de colunas divergente"

print(f"Tabela: {nome_tabela}")
print(f"Registros: {total:,}")
print(f"Municípios: {municipios:,}")
print("Silver 2023: APROVADA")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
