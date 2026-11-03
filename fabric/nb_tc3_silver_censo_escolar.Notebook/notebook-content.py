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
# Definições reutilizáveis. Execute esta célula antes da célula de execução.
# Nenhuma função depende de dados ou validações de outro ano/célula.
from pyspark.sql import functions as F


def configuracao_ano(ano):
    expectativas = {2022: 105360, 2023: 103785, 2024: 103114}
    if ano not in expectativas:
        raise ValueError("Esta etapa contempla somente 2022, 2023 e 2024")
    return {
        "ano": ano,
        "caminho": (
            "Files/bronze/censo_escolar_2024/*.csv" if ano == 2024 else
            f"Files/bronze/censo_escolar_{ano}/"
            f"{ano}_microdados_ed_basica.csv"
        ),
        "tabela": f"silver_censo_escolar_{ano}",
        "registros": expectativas[ano],
        "municipios": 5570,
    }


def contrato_silver():
    # Ordem e tipos idênticos aos das tabelas históricas.
    return (
        ("CO_ENTIDADE", "string"),
        ("CO_MUNICIPIO", "string"),
        ("NU_ANO_CENSO", "int"),
        ("TP_SITUACAO_FUNCIONAMENTO", "int"),
        ("TP_LOCALIZACAO", "int"),
        ("IN_INTERNET", "int"),
        ("IN_BIBLIOTECA", "int"),
        ("IN_SALA_LEITURA", "int"),
        ("QT_MAT_FUND_AI", "int"),
        ("QT_MAT_FUND_AI_INT", "int"),
        ("QT_DOC_FUND_AI", "int"),
        ("QT_TUR_FUND_AI", "int"),
    )


def validar_colunas_bronze(df):
    ausentes = [nome for nome, _ in contrato_silver() if nome not in df.columns]
    duplicadas = sorted({nome for nome in df.columns if df.columns.count(nome) > 1})
    if ausentes or duplicadas:
        raise ValueError(
            f"Esquema Bronze inválido: ausentes={ausentes}; duplicadas={duplicadas}"
        )


def ler_bronze(spark, ano):
    config = configuracao_ano(ano)
    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("inferSchema", False)
        .csv(config["caminho"])
    )
    validar_colunas_bronze(df)
    return df


def inteiro_seguro(nome):
    # SQL try_cast funciona com ANSI ligado ou desligado, sem alterar a sessão.
    # Os nomes vêm exclusivamente do contrato fixo do notebook.
    return F.expr(f"try_cast(`{nome}` AS INT)")


def validar_conversoes_inteiras(df_bronze):
    validar_colunas_bronze(df_bronze)
    numericas = [nome for nome, tipo in contrato_silver() if tipo == "int"]
    # Verificar toda a origem, inclusive as duas colunas usadas nos filtros,
    # antes que uma conversão inválida possa excluir silenciosamente uma linha.
    falhas = df_bronze.agg(*[
        F.count(F.when(
            F.col(nome).isNotNull()
            & (F.trim(F.col(nome)) != "")
            & inteiro_seguro(nome).isNull(),
            1,
        )).alias(nome)
        for nome in numericas
    ]).first().asDict()
    falhas = {nome: quantidade for nome, quantidade in falhas.items() if quantidade > 0}
    if falhas:
        raise ValueError(
            "Conversões para inteiro inválidas na Bronze "
            f"(valores não vazios ou fora do intervalo INT): {falhas}"
        )
    return falhas


def transformar_silver(df_bronze):
    validar_colunas_bronze(df_bronze)
    validar_conversoes_inteiras(df_bronze)
    # Mesmos filtros e tipos. try_cast mantém ausências como NULL mesmo com
    # ANSI habilitado. Valores não vazios inválidos já foram bloqueados acima.
    # Não preencher nulos nem deduplicar registros.
    return (
        df_bronze
        .filter(
            (inteiro_seguro("TP_SITUACAO_FUNCIONAMENTO") == 1)
            & (inteiro_seguro("QT_MAT_FUND_AI") > 0)
        )
        .select(*[
            F.trim(F.col(nome)).alias(nome)
            if tipo == "string" else inteiro_seguro(nome).alias(nome)
            for nome, tipo in contrato_silver()
        ])
    )


def criar_regras_consistencia(ano):
    # Novo dicionário por chamada: nenhuma mutação compartilhada entre anos.
    return {
        f"Ano diferente de {ano}": F.col("NU_ANO_CENSO") != ano,
        "Escola fora de funcionamento": F.col("TP_SITUACAO_FUNCIONAMENTO") != 1,
        "Matrículas não positivas": F.col("QT_MAT_FUND_AI") <= 0,
        "Matrículas integrais negativas": F.col("QT_MAT_FUND_AI_INT") < 0,
        "Matrículas integrais acima do total": (
            F.col("QT_MAT_FUND_AI_INT") > F.col("QT_MAT_FUND_AI")
        ),
        "Docentes negativos": F.col("QT_DOC_FUND_AI") < 0,
        "Turmas negativas": F.col("QT_TUR_FUND_AI") < 0,
        "Localização inválida": ~F.col("TP_LOCALIZACAO").isin(1, 2),
        "Indicadores binários inválidos": (
            ~F.col("IN_INTERNET").isin(0, 1)
            | ~F.col("IN_BIBLIOTECA").isin(0, 1)
            | ~F.col("IN_SALA_LEITURA").isin(0, 1)
        ),
    }


def validar_silver(df, ano):
    config = configuracao_ano(ano)
    esquema = tuple((campo.name, campo.dataType.simpleString()) for campo in df.schema)
    if esquema != contrato_silver():
        raise ValueError(f"Esquema Silver divergente em {ano}: {esquema}")

    regras = criar_regras_consistencia(ano)
    # Nulos em atributos são informativos, inclusive QT_DOC_FUND_AI.
    # Apenas os componentes da chave são obrigatórios; as regras de domínio
    # preservam a semântica original das comparações Spark com NULL.
    chave_invalida = (
        F.col("CO_ENTIDADE").isNull()
        | (F.trim(F.col("CO_ENTIDADE")) == "")
        | F.col("NU_ANO_CENSO").isNull()
    )
    contagens = df.agg(
        F.count("*").alias("registros"),
        F.countDistinct("CO_ENTIDADE").alias("escolas"),
        F.countDistinct("CO_ENTIDADE", "NU_ANO_CENSO").alias("chaves_distintas"),
        # struct mantém a semântica do select(...).distinct().count() original,
        # incluindo eventual município NULL como um valor distinto.
        F.countDistinct(F.struct("CO_MUNICIPIO")).alias("municipios"),
        F.count(F.when(chave_invalida, 1)).alias("chaves_invalidas"),
        *[
            F.count(F.when(F.col(nome).isNull(), 1)).alias(f"nulo_{indice}")
            for indice, (nome, _) in enumerate(contrato_silver())
        ],
        *[
            F.count(F.when(condicao, 1)).alias(f"erro_{indice}")
            for indice, condicao in enumerate(regras.values())
        ],
    ).first().asDict()

    nulos = {
        nome: contagens[f"nulo_{indice}"]
        for indice, (nome, _) in enumerate(contrato_silver())
    }
    inconsistencias = {
        nome: contagens[f"erro_{indice}"]
        for indice, nome in enumerate(regras)
    }
    erros = []
    if contagens["chaves_invalidas"]:
        erros.append("Chaves nulas ou vazias")
    if contagens["chaves_distintas"] != contagens["registros"]:
        erros.append("Granularidade escola/ano divergente")
    if contagens["escolas"] != contagens["registros"]:
        erros.append("Escolas duplicadas no ano")
    if contagens["registros"] != config["registros"]:
        erros.append(f"Quantidade de registros divergente: esperado {config['registros']}")
    if contagens["municipios"] != config["municipios"]:
        erros.append(f"Quantidade de municípios divergente: esperado {config['municipios']}")
    erros.extend(nome for nome, quantidade in inconsistencias.items() if quantidade > 0)
    relatorio = {
        "ano": ano,
        "registros": contagens["registros"],
        "escolas": contagens["escolas"],
        "municipios": contagens["municipios"],
        "colunas": len(df.columns),
        "chaves_distintas": contagens["chaves_distintas"],
        "chaves_invalidas": contagens["chaves_invalidas"],
        "nulos": nulos,
        "inconsistencias": inconsistencias,
    }
    if erros:
        raise ValueError(f"Silver {ano} reprovada: {erros}. Relatório: {relatorio}")
    return relatorio


def persistir_delta(spark, df, ano, *, habilitar=False):
    if habilitar is not True:
        raise ValueError("Persistência desabilitada; habilitação explícita obrigatória")
    # Revalidar o próprio DataFrame recebido, sem confiar em relatório antigo.
    validar_silver(df, ano)
    nome_tabela = configuracao_ano(ano)["tabela"]
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela)
    )
    return validar_silver(spark.table(nome_tabela), ano)


def processar_anos(spark, anos, *, habilitar_persistencia=False):
    # Estado local por execução. Validar todos os anos antes de qualquer escrita.
    anos = tuple(anos)
    if not anos or len(set(anos)) != len(anos):
        raise ValueError("Informe anos distintos e uma seleção não vazia")
    for ano in anos:
        configuracao_ano(ano)
    resultados = {}
    for ano in anos:
        df_bronze = ler_bronze(spark, ano)
        df_silver = transformar_silver(df_bronze)
        relatorio = validar_silver(df_silver, ano)
        resultados[ano] = {"dataframe": df_silver, "validacao": relatorio}
        print(f"Silver {ano} validada: {relatorio}")

    if habilitar_persistencia is True:
        # Escritas anuais independentes: não há transação entre as duas tabelas.
        for ano, resultado in resultados.items():
            resultado["persistencia"] = persistir_delta(
                spark, resultado["dataframe"], ano, habilitar=True
            )
    else:
        print("Persistência desabilitada. Tabelas existentes preservadas.")
    return resultados

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executar após a célula de definições; não depende de execução de outro ano.
# Primeira execução: manter False e revisar os relatórios de todos os anos.
# Somente após aprovação, alterar explicitamente para True para regravar as
# tabelas anuais existentes. A validação obrigatória será executada novamente.
resultados = processar_anos(
    spark,
    anos=(2022, 2023, 2024),
    habilitar_persistencia=False,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
