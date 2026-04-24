import duckdb
import os

# CONFIGURAÇÕES
arquivo_origem = r"C:\inativos_ben\sib_inativo_SP.csv"
pasta_saida = r"C:\inativos_ben\saida_trimestres"
banco_local = r"C:\inativos_ben\processamento_ans.db"  # caminho absoluto

if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

con = duckdb.connect(banco_local)

print("--- Passo 1: Lendo e Higienizando ---")

con.execute(f"""
    CREATE OR REPLACE TABLE base_bruta AS
    SELECT * FROM read_csv(
        '{arquivo_origem.replace("\\", "/")}',
        delim=';',
        header=True,
        all_varchar=True
    )
""")

# Diagnóstico rápido (amostra) para confirmar formato real:
print("--- Amostra DT_CONTRATACAO / DT_CANCELAMENTO (raw) ---")
amostra = con.execute(r"""
    SELECT
        DT_CONTRATACAO,
        DT_CANCELAMENTO
    FROM base_bruta
    WHERE (DT_CONTRATACAO IS NOT NULL AND trim(DT_CONTRATACAO) <> '')
       OR (DT_CANCELAMENTO IS NOT NULL AND trim(DT_CANCELAMENTO) <> '')
    LIMIT 20
""").fetchall()
for row in amostra:
    print(row)

# Parsing robusto com try_strptime:
# - prioriza dd/mm/aaaa
# - aceita timestamp com hora
# - fallback para yyyy-mm-dd e variações
# - fallback para strings numéricas (pega só os 8 primeiros dígitos)
#
# DuckDB: strptime/try_strptime + exemplos de %d/%m/%Y estão na documentação. [1](https://duckdb.org/docs/current/sql/functions/dateformat)
# DuckDB: regexp_replace com flag 'g' (global) é suportado. [2](https://duckdb.org/docs/current/sql/functions/regular_expressions)
con.execute(r"""
con.execute(r"""
CREATE OR REPLACE TABLE base_limpa AS
WITH src AS (
    SELECT
        *,
        regexp_replace(DT_NASCIMENTO,   '[^0-9]', '', 'g') AS nasc_num,
        regexp_replace(DT_CONTRATACAO,  '[^0-9]', '', 'g') AS cont_num,
        regexp_replace(DT_CANCELAMENTO, '[^0-9]', '', 'g') AS canc_num
    FROM base_bruta
),
conv AS (
    SELECT
        COALESCE(NULLIF(trim(CD_PLANO_RPS), ''), 'PRODUTO NÃO IDENTIFICADO') AS Produto,
        REGISTRO_OPERADORA,
        CD_MUNICIPIO,
        TP_SEXO,

        -- NASCIMENTO: aceita AAAA, YYYY-MM, dd/mm/aaaa, yyyy-mm-dd (com/sem hora)
        CASE
            WHEN DT_NASCIMENTO IS NULL OR trim(DT_NASCIMENTO) = '' THEN NULL
            WHEN length(nasc_num) = 4 THEN try_cast(nasc_num || '-01-01' AS DATE)

            -- YYYY-MM  -> YYYY-MM-01
            WHEN regexp_full_match(trim(DT_NASCIMENTO), '^\d{4}-\d{2}$')
                THEN try_cast(trim(DT_NASCIMENTO) || '-01' AS DATE)

            ELSE
                CAST(
                    coalesce(
                        try_strptime(trim(DT_NASCIMENTO), '%d/%m/%Y'),
                        try_strptime(trim(DT_NASCIMENTO), '%d/%m/%Y %H:%M:%S'),
                        try_strptime(trim(DT_NASCIMENTO), '%Y-%m-%d'),
                        try_strptime(trim(DT_NASCIMENTO), '%Y-%m-%d %H:%M:%S'),

                        -- só tenta numérico se tiver pelo menos 8 dígitos
                        CASE WHEN length(nasc_num) >= 8 THEN try_strptime(substr(nasc_num, 1, 8), '%d%m%Y') END,
                        CASE WHEN length(nasc_num) >= 8 THEN try_strptime(substr(nasc_num, 1, 8), '%Y%m%d') END
                    ) AS DATE
                )
        END AS DT_NASCIMENTO,

        -- CONTRATAÇÃO: principal é YYYY-MM; secundário dd/mm/aaaa etc.
        CASE
            WHEN DT_CONTRATACAO IS NULL OR trim(DT_CONTRATACAO) = '' THEN NULL

            -- YYYY-MM -> YYYY-MM-01
            WHEN regexp_full_match(trim(DT_CONTRATACAO), '^\d{4}-\d{2}$')
                THEN try_cast(trim(DT_CONTRATACAO) || '-01' AS DATE)

            ELSE
                CAST(
                    coalesce(
                        try_strptime(trim(DT_CONTRATACAO), '%d/%m/%Y'),
                        try_strptime(trim(DT_CONTRATACAO), '%d/%m/%Y %H:%M:%S'),
                        try_strptime(trim(DT_CONTRATACAO), '%Y-%m-%d'),
                        try_strptime(trim(DT_CONTRATACAO), '%Y-%m-%d %H:%M:%S'),

                        CASE WHEN length(cont_num) >= 8 THEN try_strptime(substr(cont_num, 1, 8), '%d%m%Y') END,
                        CASE WHEN length(cont_num) >= 8 THEN try_strptime(substr(cont_num, 1, 8), '%Y%m%d') END
                    ) AS DATE
                )
        END AS DT_CONTRATACAO,

        -- CANCELAMENTO: se YYYY-MM, usa ÚLTIMO dia do mês (convenção p/ "ativo no mês")
        CASE
            WHEN DT_CANCELAMENTO IS NULL OR trim(DT_CANCELAMENTO) = '' THEN NULL

            WHEN regexp_full_match(trim(DT_CANCELAMENTO), '^\d{4}-\d{2}$') THEN
                CAST(
                    (date_trunc('month', try_cast(trim(DT_CANCELAMENTO) || '-01' AS DATE))
                     + INTERVAL '1 month' - INTERVAL '1 day') AS DATE
                )

            ELSE
                CAST(
                    coalesce(
                        try_strptime(trim(DT_CANCELAMENTO), '%d/%m/%Y'),
                        try_strptime(trim(DT_CANCELAMENTO), '%d/%m/%Y %H:%M:%S'),
                        try_strptime(trim(DT_CANCELAMENTO), '%Y-%m-%d'),
                        try_strptime(trim(DT_CANCELAMENTO), '%Y-%m-%d %H:%M:%S'),

                        CASE WHEN length(canc_num) >= 8 THEN try_strptime(substr(canc_num, 1, 8), '%d%m%Y') END,
                        CASE WHEN length(canc_num) >= 8 THEN try_strptime(substr(canc_num, 1, 8), '%Y%m%d') END
                    ) AS DATE
                )
        END AS DT_CANCELAMENTO

    FROM src
)
SELECT * FROM conv
""")

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

# Agora inclui 3T2024 e 4T2024 (antes você realmente não tinha esses itens na lista)
trimestres = [
    ('1T2018', '2018-03-31'), ('2T2018', '2018-06-30'), ('3T2018', '2018-09-30'), ('4T2018', '2018-12-31'),
    ('1T2019', '2019-03-31'), ('2T2019', '2019-06-30'), ('3T2019', '2019-09-30'), ('4T2019', '2019-12-31'),
    ('1T2020', '2020-03-31'), ('2T2020', '2020-06-30'), ('3T2020', '2020-09-30'), ('4T2020', '2020-12-31'),
    ('1T2021', '2021-03-31'), ('2T2021', '2021-06-30'), ('3T2021', '2021-09-30'), ('4T2021', '2021-12-31'),
    ('1T2022', '2022-03-31'), ('2T2022', '2022-06-30'), ('3T2022', '2022-09-30'), ('4T2022', '2022-12-31'),
    ('1T2023', '2023-03-31'), ('2T2023', '2023-06-30'), ('3T2023', '2023-09-30'), ('4T2023', '2023-12-31'),
    ('1T2024', '2024-03-31'), ('2T2024', '2024-06-30'),
    ('3T2024', '2024-09-30'), ('4T2024', '2024-12-31')
]

for nome, data_ref in trimestres:
    arquivo_csv = os.path.join(pasta_saida, f"ativos_{nome}.csv").replace("\\", "/")

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

