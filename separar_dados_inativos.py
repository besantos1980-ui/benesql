import duckdb
import os

# CONFIGURAÇÕES
arquivo_origem = r"C:\inativos_ben\sib_inativo_SP.csv"
pasta_saida = r"C:\inativos_ben\saida_trimestres"
banco_local = r"C:\inativos_ben\processamento_ans.db"  # fixo e absoluto para não abrir db errado

if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

con = duckdb.connect(banco_local)

print("--- Passo 1: Lendo e Higienizando ---")

# 1) Carrega o CSV cru
con.execute(f"""
    CREATE OR REPLACE TABLE base_bruta AS
    SELECT * FROM read_csv(
        '{arquivo_origem.replace("\\", "/")}',
        delim=';',
        header=True,
        all_varchar=True
    )
""")

# 2) Cria base_limpa com parsing robusto (BR -> ISO)
#    - Remove tudo que não é dígito
#    - Para 8 dígitos assume DDMMAAAA (padrão comum em bases nacionais)
#    - Para 4 dígitos em nascimento assume ano (AAAA -> AAAA-01-01)
con.execute(r"""
CREATE OR REPLACE TABLE base_limpa AS
WITH src AS (
    SELECT
        *,
        regexp_replace(DT_NASCIMENTO,   '[^0-9]', '', 'g') AS nasc_num,
        regexp_replace(DT_CONTRATACAO,  '[^0-9]', '', 'g') AS cont_num,
        regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g') AS canc_num
    FROM base_bruta
)
SELECT
    -- Garante que produtos vazios não sumam no pivot
    COALESCE(NULLIF(trim(CD_PLANO_RPS), ''), 'PRODUTO NÃO IDENTIFICADO') AS Produto,
    REGISTRO_OPERADORA,
    CD_MUNICIPIO,
    TP_SEXO,

    -- NASCIMENTO
    CASE
        WHEN nasc_num IS NULL OR nasc_num = '' THEN NULL
        WHEN length(nasc_num) = 4 THEN CAST(nasc_num || '-01-01' AS DATE)
        WHEN length(nasc_num) = 8 THEN
            CAST(substr(nasc_num,5,4) || '-' || substr(nasc_num,3,2) || '-' || substr(nasc_num,1,2) AS DATE)
        ELSE NULL
    END AS DT_NASCIMENTO,

    -- CONTRATACAO
    CASE
        WHEN cont_num IS NULL OR cont_num = '' THEN NULL
        WHEN length(cont_num) = 8 THEN
            CAST(substr(cont_num,5,4) || '-' || substr(cont_num,3,2) || '-' || substr(cont_num,1,2) AS DATE)
        ELSE NULL
    END AS DT_CONTRATACAO,

    -- CANCELAMENTO
    CASE
        WHEN canc_num IS NULL OR canc_num = '' THEN NULL
        WHEN length(canc_num) = 8 THEN
            CAST(substr(canc_num,5,4) || '-' || substr(canc_num,3,2) || '-' || substr(canc_num,1,2) AS DATE)
        ELSE NULL
    END AS DT_CANCELAMENTO

FROM src
""")

con.execute("DROP TABLE base_bruta")

# 3) Validações rápidas (para não exportar vazio sem saber)
print("--- Validação: datas após parsing ---")
total = con.execute("select count(*) from base_limpa").fetchone()[0]
cont_ok = con.execute("select count(*) from base_limpa where DT_CONTRATACAO is not null").fetchone()[0]
canc_ok = con.execute("select count(*) from base_limpa where DT_CANCELAMENTO is not null").fetchone()[0]
minmax_cont = con.execute("select min(DT_CONTRATACAO), max(DT_CONTRATACAO) from base_limpa").fetchone()
minmax_canc = con.execute("select min(DT_CANCELAMENTO), max(DT_CANCELAMENTO) from base_limpa").fetchone()

print(f"Total linhas: {total}")
print(f"DT_CONTRATACAO preenchida: {cont_ok} ({cont_ok/total:.2%})")
print(f"DT_CANCELAMENTO preenchida: {canc_ok} ({canc_ok/total:.2%})")
print("Min/Max DT_CONTRATACAO:", minmax_cont)
print("Min/Max DT_CANCELAMENTO:", minmax_canc)

print("--- Passo 2: Exportando CSVs ---")

trimestres = [
    ('1T2018', '2018-03-31'), ('2T2018', '2018-06-30'), ('3T2018', '2018-09-30'), ('4T2018', '2018-12-31'),
    ('1T2019', '2019-03-31'), ('2T2019', '2019-06-30'), ('3T2019', '2019-09-30'), ('4T2019', '2019-12-31'),
    ('1T2020', '2020-03-31'), ('2T2020', '2020-06-30'), ('3T2020', '2020-09-30'), ('4T2020', '2020-12-31'),
    ('1T2021', '2021-03-31'), ('2T2021', '2021-06-30'), ('3T2021', '2021-09-30'), ('4T2021', '2021-12-31'),
    ('1T2022', '2022-03-31'), ('2T2022', '2022-06-30'), ('3T2022', '2022-09-30'), ('4T2022', '2022-12-31'),
    ('1T2023', '2023-03-31'), ('2T2023', '2023-06-30'), ('3T2023', '2023-09-30'), ('4T2023', '2023-12-31'),
    ('1T2024', '2024-03-31'), ('2T2024', '2024-06-30')
]

for nome, data_ref in trimestres:
    arquivo_csv = os.path.join(pasta_saida, f"ativos_{nome}.csv").replace("\\", "/")

    # (Opcional, mas recomendado) Log de linhas elegíveis por trimestre
    elegiveis = con.execute(f"""
        SELECT count(*)
        FROM base_limpa
        WHERE DT_CONTRATACAO <= DATE '{data_ref}'
          AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > DATE '{data_ref}')
    """).fetchone()[0]

    print(f"{nome} ({data_ref}) -> linhas elegíveis: {elegiveis}")

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
                END AS Faixa_Etaria,
                COUNT(*) AS Total
            FROM base_limpa
            WHERE DT_CONTRATACAO <= DATE '{data_ref}'
              AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > DATE '{data_ref}')
            GROUP BY 1, 2
        ) TO '{arquivo_csv}' (HEADER, DELIMITER ';')
    """)

print("Trimestres gerados.")
con.close()
``
