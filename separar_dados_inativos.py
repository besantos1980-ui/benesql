import duckdb
import pandas as pd
import os

# 1. Configurações de Caminho
arquivo_origem = r"C:/inativos_ben/seu_arquivo.csv" # Use o 'r' antes das aspas para caminhos Windows
arquivo_saida = "Relatorio_Beneficiarios_Trimestral.xlsx"

# Conecta ao DuckDB (criando um arquivo de banco local para não estourar a RAM)
con = duckdb.connect('cache_processamento.db')

print("--- Passo 1: Criando cache otimizado (Isso pode demorar um pouco, mas só roda 1 vez) ---")

# Criamos uma tabela temporária tratando as datas "sujas"
# O try_cast converte o que for possível e retorna NULL para o que for inválido (como '2012-07')
con.execute(f"""
    CREATE OR REPLACE TABLE base_limpa AS 
    SELECT 
        REGISTRO_OPERADORA,
        CD_PLANO_RPS,
        CD_MUNICIPIO,
        TP_SEXO,
        try_cast(DT_NASCIMENTO AS DATE) as DT_NASCIMENTO,
        try_cast(DT_CONTRATACAO AS DATE) as DT_CONTRATACAO,
        try_cast(DT_CANCELAMENTO AS DATE) as DT_CANCELAMENTO
    FROM read_csv('{arquivo_origem}', 
                  delim=';', 
                  header=True, 
                  all_varchar=True) -- Lê tudo como texto primeiro para evitar erro de tipo
""")

print("--- Passo 2: Gerando Abas por Trimestre ---")

trimestres = [
    ('1T2018', '2018-03-31'), ('2T2018', '2018-06-30'), ('3T2018', '2018-09-30'), ('4T2018', '2018-12-31'),
    ('1T2019', '2019-03-31'), ('2T2019', '2019-06-30'), ('3T2019', '2019-09-30'), ('4T2019', '2019-12-31'),
    ('1T2020', '2020-03-31'), ('2T2020', '2020-06-30'), ('3T2020', '2020-09-30'), ('4T2020', '2020-12-31'),
    ('1T2021', '2021-03-31'), ('2T2021', '2021-06-30'), ('3T2021', '2021-09-30'), ('4T2021', '2021-12-31'),
    ('1T2022', '2022-03-31'), ('2T2022', '2022-06-30'), ('3T2022', '2022-09-30'), ('4T2022', '2022-12-31'),
    ('1T2023', '2023-03-31'), ('2T2023', '2023-06-30'), ('3T2023', '2023-09-30'), ('4T2023', '2023-12-31'),
    ('1T2024', '2024-03-31'), ('2T2024', '2024-06-30')
]

with pd.ExcelWriter(arquivo_saida, engine='xlsxwriter') as writer:
    for nome_aba, data_corte in trimestres:
        print(f"Processando {nome_aba}...")
        
        query = f"""
        WITH ativos AS (
            SELECT 
                REGISTRO_OPERADORA, CD_PLANO_RPS, CD_MUNICIPIO, TP_SEXO,
                date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) as idade
            FROM base_limpa
            WHERE DT_CONTRATACAO <= '{data_corte}'
              AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > '{data_corte}')
        ),
        agrupado AS (
            SELECT 
                REGISTRO_OPERADORA, CD_PLANO_RPS, CD_MUNICIPIO, TP_SEXO,
                CASE 
                    WHEN idade < 1 THEN '<1 ano'
                    WHEN idade BETWEEN 1 AND 4 THEN '1 a 4 anos'
                    WHEN idade BETWEEN 5 AND 9 THEN '5 a 9 anos'
                    WHEN idade BETWEEN 10 AND 14 THEN '10 a 14 anos'
                    WHEN idade BETWEEN 15 AND 19 THEN '15 a 19 anos'
                    WHEN idade BETWEEN 20 AND 29 THEN '20 a 29 anos'
                    WHEN idade BETWEEN 30 AND 39 THEN '30 a 39 anos'
                    WHEN idade BETWEEN 40 AND 49 THEN '40 a 49 anos'
                    WHEN idade BETWEEN 50 AND 59 THEN '50 a 59 anos'
                    WHEN idade BETWEEN 60 AND 69 THEN '60 a 69 anos'
                    WHEN idade BETWEEN 70 AND 79 THEN '70 a 79 anos'
                    ELSE '80 anos ou mais'
                END as FAIXA_ETARIA
            FROM ativos
        )
        SELECT 
            REGISTRO_OPERADORA as Operadora,
            CD_PLANO_RPS as Produto,
            CD_MUNICIPIO as Municipio,
            TP_SEXO as Sexo,
            FAIXA_ETARIA as Faixa_Etaria,
            COUNT(*) as Total_Beneficiarios
        FROM agrupado
        GROUP BY 1, 2, 3, 4, 5
        """
        
        df = con.execute(query).df()
        df.to_excel(writer, sheet_name=nome_aba, index=False)

print(f"Concluído! Arquivo salvo em: {arquivo_saida}")
