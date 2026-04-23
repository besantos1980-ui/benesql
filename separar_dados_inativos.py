import duckdb
import os

# 1. Configurações de Caminho
arquivo_origem = r"C:\inativos_ben\seu_arquivo.csv"
pasta_saida = r"C:\inativos_ben\saida_trimestres"

# Cria a pasta de saída se não existir
if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

# Conecta ao DuckDB criando um arquivo de banco (evita estourar a RAM com 216M de linhas)
con = duckdb.connect('processamento_ans.db')

print("--- Passo 1: Lendo e Higienizando 216 Milhões de Linhas ---")

# A estratégia de COALESCE com REGEXP garante que não perderemos dados por erro de formato
con.execute(r"""
    CREATE OR REPLACE TABLE base_limpa AS 
    SELECT 
        REGISTRO_OPERADORA,
        CD_PLANO_RPS,
        CD_MUNICIPIO,
        TP_SEXO,
        COALESCE(
            try_cast(strptime(DT_NASCIMENTO, '%d/%m/%Y') AS DATE),
            try_cast(DT_NASCIMENTO AS DATE),
            try_cast(regexp_extract(DT_NASCIMENTO, '(\d{4})', 1) || '-01-01' AS DATE)
        ) as DT_NASCIMENTO,
        COALESCE(
            try_cast(strptime(DT_CONTRATACAO, '%d/%m/%Y') AS DATE),
            try_cast(DT_CONTRATACAO AS DATE),
            try_cast(regexp_extract(DT_CONTRATACAO, '(\d{4})', 1) || '-01-01' AS DATE)
        ) as DT_CONTRATACAO,
        COALESCE(
            try_cast(strptime(DT_CANCELAMENTO, '%d/%m/%Y') AS DATE),
            try_cast(DT_CANCELAMENTO AS DATE),
            try_cast(regexp_extract(DT_CANCELAMENTO, '(\d{4})', 1) || '-01-01' AS DATE)
        ) as DT_CANCELAMENTO
    FROM read_csv('""" + arquivo_origem.replace('\\', '/') + r"""', 
                  delim=';', 
                  header=True, 
                  all_varchar=True)
""")

# Diagnóstico para conferência no console
check = con.execute("SELECT COUNT(*), COUNT(DT_CONTRATACAO) FROM base_limpa").fetchone()
print(f"Total de linhas lidas: {check[0]}")
print(f"Linhas com data de contratação válida: {check[1]}")

print("--- Passo 2: Exportando CSVs por Trimestre ---")

trimestres = [
    ('1T2018', '2018-03-31'), ('2T2018', '2018-06-30'), ('3T2018', '2018-09-30'), ('4T2018', '2018-12-31'),
    ('1T2019', '2019-03-31'), ('2T2019', '2019-06-30'), ('3T2019', '2019-09-30'), ('4T2019', '2019-12-31'),
    ('1T2020', '2020-03-31'), ('2T2020', '2020-06-30'), ('3T2020', '2020-09-30'), ('4T2020', '2020-12-31'),
    ('1T2021', '2021-03-31'), ('2T2021', '2021-06-30'), ('3T2021', '2021-09-30'), ('4T2021', '2021-12-31'),
    ('1T2022', '2022-03-31'), ('2T2022', '2022-06-30'), ('3T2022', '2022-09-30'), ('4T2022', '2022-12-31'),
    ('1T2023', '2023-03-31'), ('2T2023', '2023-06-30'), ('3T2023', '2023-09-30'), ('4T2023', '2023-12-31'),
    ('1T2024', '2024-03-31'), ('2T2024', '2024-06-30')
]

for nome_aba, data_corte in trimestres:
    arquivo_csv = os.path.join(pasta_saida, f"beneficiarios_ativos_{nome_aba}.csv")
    print(f"Gerando {arquivo_csv}...")
    
    # Exportação direta do DuckDB para CSV (muito mais rápido que passar pelo Pandas)
    con.execute(f"""
        COPY (
            SELECT 
                REGISTRO_OPERADORA as Operadora,
                CD_PLANO_RPS as Produto,
                CD_MUNICIPIO as Municipio,
                TP_SEXO as Sexo,
                CASE 
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) < 1 THEN '<1 ano'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 1 AND 4 THEN '1 a 4 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 5 AND 9 THEN '5 a 9 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 10 AND 14 THEN '10 a 14 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 15 AND 19 THEN '15 a 19 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 20 AND 29 THEN '20 a 29 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 30 AND 39 THEN '30 a 39 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 40 AND 49 THEN '40 a 49 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 50 AND 59 THEN '50 a 59 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 60 AND 69 THEN '60 a 69 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) BETWEEN 70 AND 79 THEN '70 a 79 anos'
                    ELSE '80 anos ou mais'
                END as Faixa_Etaria,
                COUNT(*) as Total_Beneficiarios
            FROM base_limpa
            WHERE DT_CONTRATACAO <= '{data_corte}'
              AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > '{data_corte}')
            GROUP BY 1, 2, 3, 4, 5
        ) TO '{arquivo_csv.replace('\\', '/')}' (HEADER, DELIMITER ';')
    """)

print("--- Processamento concluído com sucesso! ---")
