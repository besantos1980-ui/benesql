import duckdb
import os

arquivo_origem = r"C:\inativos_ben\sib_inativo_SP.csv"
pasta_saida = r"C:\inativos_ben\saida_trimestres"
banco_local = 'processamento_ans.db'

if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

con = duckdb.connect(banco_local)

print("--- Passo 1: Lendo e Higienizando ---")

con.execute(f"""
    CREATE OR REPLACE TABLE base_bruta AS 
    SELECT * FROM read_csv('{arquivo_origem.replace('\\', '/')}', 
                  delim=';', header=True, all_varchar=True)
""")

con.execute(r"""
    CREATE OR REPLACE TABLE base_limpa AS 
    SELECT 
        -- Tratando produtos nulos ou vazios para não serem perdidos
        COALESCE(NULLIF(trim(CD_PLANO_RPS), ''), 'NÃO IDENTIFICADO') as Produto,
        REGISTRO_OPERADORA,
        CD_MUNICIPIO,
        TP_SEXO,
        CASE 
            WHEN length(regexp_replace(DT_NASCIMENTO, '[^0-9]', '', 'g')) = 4 
            THEN CAST(regexp_replace(DT_NASCIMENTO, '[^0-9]', '', 'g') || '-01-01' AS DATE)
            ELSE try_cast(DT_NASCIMENTO AS DATE)
        END as DT_NASCIMENTO,
        CASE 
            WHEN length(regexp_replace(DT_CONTRATACAO, '[^0-9]', '', 'g')) = 8 
            THEN CAST(substring(regexp_replace(DT_CONTRATACAO, '[^0-9]', '', 'g'), 5, 4) || '-' ||
                      substring(regexp_replace(DT_CONTRATACAO, '[^0-9]', '', 'g'), 3, 2) || '-' ||
                      substring(regexp_replace(DT_CONTRATACAO, '[^0-9]', '', 'g'), 1, 2) AS DATE)
            ELSE try_cast(DT_CONTRATACAO AS DATE)
        END as DT_CONTRATACAO,
        CASE 
            WHEN length(regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g')) = 8 
            THEN CAST(substring(regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g'), 5, 4) || '-' ||
                      substring(regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g'), 3, 2) || '-' ||
                      substring(regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g'), 1, 2) AS DATE)
            ELSE try_cast(DT_CANCELAMENTO AS DATE)
        END as DT_CANCELAMENTO
    FROM base_bruta
""")

con.execute("DROP TABLE base_bruta")

print("--- Passo 2: Exportando CSVs corrigidos ---")

trimestres = [('1T2018', '2018-03-31'), ('2T2018', '2018-06-30'), ('3T2018', '2018-09-30'), ('4T2018', '2018-12-31'),
              ('1T2019', '2019-03-31'), ('2T2019', '2019-06-30'), ('3T2019', '2019-09-30'), ('4T2019', '2019-12-31'),
              ('1T2020', '2020-03-31'), ('2T2020', '2020-06-30'), ('3T2020', '2020-09-30'), ('4T2020', '2020-12-31'),
              ('1T2021', '2021-03-31'), ('2T2021', '2021-06-30'), ('3T2021', '2021-09-30'), ('4T2021', '2021-12-31'),
              ('1T2022', '2022-03-31'), ('2T2022', '2022-06-30'), ('3T2022', '2022-09-30'), ('4T2022', '2022-12-31'),
              ('1T2023', '2023-03-31'), ('2T2023', '2023-06-30'), ('3T2023', '2023-09-30'), ('4T2023', '2023-12-31'),
              ('1T2024', '2024-03-31'), ('2T2024', '2024-06-30')]

for nome, data_ref in trimestres:
    arquivo_csv = os.path.join(pasta_saida, f"ativos_{nome}.csv")
    con.execute(f"""
        COPY (
            SELECT 
                Produto,
                CASE 
                    WHEN DT_NASCIMENTO IS NULL THEN 'Idade Desconhecida'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') < 1 THEN '<1 ano'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 1 AND 4 THEN '1 a 4 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 5 AND 9 THEN '5 a 9 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 10 AND 14 THEN '10 a 14 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 15 AND 19 THEN '15 a 19 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 20 AND 29 THEN '20 a 29 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 30 AND 39 THEN '30 a 39 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 40 AND 49 THEN '40 a 49 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 50 AND 59 THEN '50 a 59 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 60 AND 69 THEN '60 a 69 anos'
                    WHEN date_diff('year', DT_NASCIMENTO, DATE '{data_ref}') BETWEEN 70 AND 79 THEN '70 a 79 anos'
                    ELSE '80 anos ou mais'
                END as Faixa_Etaria,
                COUNT(*) as Total
            FROM base_limpa
            WHERE DT_CONTRATACAO <= '{data_ref}'
              AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > '{data_ref}')
            GROUP BY 1, 2
        ) TO '{arquivo_csv.replace('\\', '/')}' (HEADER, DELIMITER ';')
    """)
print("Trimestres gerados com sucesso.")
