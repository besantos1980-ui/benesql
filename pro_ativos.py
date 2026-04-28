import duckdb
import os

# =========================
# CONFIGURAÇÕES
# =========================
ARQUIVO_ORIGEM = r"C:\inativos_ben\sib_ativo_SP.csv"
PASTA_SAIDA = r"C:\inativos_ben\saida_trimestres_ativo"
BANCO_LOCAL = r"C:\inativos_ben\processamento_ans.db"

# Se quiser manter base_bruta para inspeção depois, deixe True
MANTER_BASE_BRUTA = False

# =========================
# PREPARO
# =========================
os.makedirs(PASTA_SAIDA, exist_ok=True)

con = duckdb.connect(BANCO_LOCAL)

print("--- Passo 1: Lendo CSV bruto ---")

con.execute(f"""
    CREATE OR REPLACE TABLE base_bruta AS
    SELECT *
    FROM read_csv(
        '{ARQUIVO_ORIGEM.replace("\\", "/")}',
        delim=';',
        header=True,
        all_varchar=True
    )
""")

# =========================
# PARSING ROBUSTO DE DATAS
# =========================
print("--- Passo 2: Criando base_limpa (parsing de datas) ---")

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
    COALESCE(NULLIF(trim(CD_PLANO_RPS), ''), 'PRODUTO NÃO IDENTIFICADO') AS Produto,
    REGISTRO_OPERADORA,
    CD_MUNICIPIO,
    TP_SEXO,

    -- =========================
    -- DT_NASCIMENTO
    -- - aceita: YYYY-MM (vira YYYY-MM-01), dd/mm/aaaa, yyyy-mm-dd, e variações com hora
    -- - sentinela 1946-01-01 permanece como data (a classificação 80+ é feita na faixa etária)
    -- =========================
    CASE
        WHEN DT_NASCIMENTO IS NULL OR trim(DT_NASCIMENTO) = '' THEN NULL

        -- YYYY (ano apenas)
        WHEN regexp_full_match(trim(DT_NASCIMENTO), '^\d{4}$')
            THEN try_cast(trim(DT_NASCIMENTO) || '-01-01' AS DATE)

        -- YYYY-MM -> YYYY-MM-01 (IMPORTANTE para preservar sentinela "1946-01" -> 1946-01-01)
        WHEN regexp_full_match(trim(DT_NASCIMENTO), '^\d{4}-\d{2}$')
            THEN try_cast(trim(DT_NASCIMENTO) || '-01' AS DATE)

        ELSE CAST(
            coalesce(
                try_strptime(trim(DT_NASCIMENTO), '%d/%m/%Y'),
                try_strptime(trim(DT_NASCIMENTO), '%d/%m/%Y %H:%M:%S'),
                try_strptime(trim(DT_NASCIMENTO), '%Y-%m-%d'),
                try_strptime(trim(DT_NASCIMENTO), '%Y-%m-%d %H:%M:%S'),

                -- fallback numérico somente se tiver 8+ dígitos
                CASE WHEN length(nasc_num) >= 8 THEN try_strptime(substr(nasc_num, 1, 8), '%d%m%Y') END,
                CASE WHEN length(nasc_num) >= 8 THEN try_strptime(substr(nasc_num, 1, 8), '%Y%m%d') END
            ) AS DATE
        )
    END AS DT_NASCIMENTO,

    -- =========================
    -- DT_CONTRATACAO
    -- - aceita: YYYY-MM (vira YYYY-MM-01), dd/mm/aaaa, yyyy-mm-dd, e variações com hora
    -- =========================
    CASE
        WHEN DT_CONTRATACAO IS NULL OR trim(DT_CONTRATACAO) = '' THEN NULL

        WHEN regexp_full_match(trim(DT_CONTRATACAO), '^\d{4}-\d{2}$')
            THEN try_cast(trim(DT_CONTRATACAO) || '-01' AS DATE)

        ELSE CAST(
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

    -- =========================
    -- DT_CANCELAMENTO
    -- - aceita: YYYY-MM (vira ÚLTIMO DIA DO MÊS), dd/mm/aaaa, yyyy-mm-dd, e variações com hora
    -- =========================
    CASE
        WHEN DT_CANCELAMENTO IS NULL OR trim(DT_CANCELAMENTO) = '' THEN NULL

        WHEN regexp_full_match(trim(DT_CANCELAMENTO), '^\d{4}-\d{2}$') THEN
            CAST(
                (date_trunc('month', try_cast(trim(DT_CANCELAMENTO) || '-01' AS DATE))
                 + INTERVAL '1 month' - INTERVAL '1 day') AS DATE
            )

        ELSE CAST(
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
""")

if not MANTER_BASE_BRUTA:
    con.execute("DROP TABLE base_bruta")

# =========================
# VALIDAÇÕES
# =========================
print("--- Validação: datas após parsing ---")
total = con.execute("select count(*) from base_limpa").fetchone()[0]
cont_ok = con.execute("select count(*) from base_limpa where DT_CONTRATACAO is not null").fetchone()[0]
canc_ok = con.execute("select count(*) from base_limpa where DT_CANCELAMENTO is not null").fetchone()[0]
nasc_ok = con.execute("select count(*) from base_limpa where DT_NASCIMENTO is not null").fetchone()[0]
minmax_nasc = con.execute("select min(DT_NASCIMENTO), max(DT_NASCIMENTO) from base_limpa where DT_NASCIMENTO is not null").fetchone()
minmax_cont = con.execute("select min(DT_CONTRATACAO), max(DT_CONTRATACAO) from base_limpa where DT_CONTRATACAO is not null").fetchone()
minmax_canc = con.execute("select min(DT_CANCELAMENTO), max(DT_CANCELAMENTO) from base_limpa where DT_CANCELAMENTO is not null").fetchone()

print(f"Total linhas: {total}")
print(f"DT_NASCIMENTO preenchida: {nasc_ok} ({nasc_ok/total:.2%})")
print(f"DT_CONTRATACAO preenchida: {cont_ok} ({cont_ok/total:.2%})")
print(f"DT_CANCELAMENTO preenchida: {canc_ok} ({canc_ok/total:.2%})")
print("Min/Max DT_NASCIMENTO:", minmax_nasc)
print("Min/Max DT_CONTRATACAO:", minmax_cont)
print("Min/Max DT_CANCELAMENTO:", minmax_canc)

# =========================
# LISTA DE TRIMESTRES
# =========================
print("--- Passo 3: Exportando CSVs por trimestre ---")

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

# Expressão de idade correta (sem CTE; vamos repetir no CASE via string)
# (não é CTE; é só um "pedaço" de SQL reaproveitado para evitar erro de digitação)
idade_expr_tpl = """
(
  EXTRACT(year FROM DATE '{data_ref}')
  - EXTRACT(year FROM DT_NASCIMENTO)
  - CASE
      WHEN strftime(DATE '{data_ref}', '%m%d') < strftime(DT_NASCIMENTO, '%m%d')
      THEN 1 ELSE 0
    END
)
"""

for nome, data_ref in trimestres:
    arquivo_csv = os.path.join(PASTA_SAIDA, f"ativosatual_{nome}.csv").replace("\\", "/")
    idade_expr = idade_expr_tpl.format(data_ref=data_ref)

    # Log de elegíveis (evita gerar arquivo vazio "sem saber")
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

                    -- Regra de negócio: sentinela = 80+
                    WHEN DT_NASCIMENTO = DATE '1946-01-01' THEN '80 anos ou mais'

                    -- Proteção: nascimento futuro em relação à data de referência
                    WHEN DT_NASCIMENTO > DATE '{data_ref}' THEN 'Idade Desconhecida'

                    -- Faixas pela idade "real" (para os demais)
                    WHEN {idade_expr} < 1 THEN '<1 ano'
                    WHEN {idade_expr} BETWEEN 1 AND 4 THEN '1 a 4 anos'
                    WHEN {idade_expr} BETWEEN 5 AND 9 THEN '5 a 9 anos'
                    WHEN {idade_expr} BETWEEN 10 AND 14 THEN '10 a 14 anos'
                    WHEN {idade_expr} BETWEEN 15 AND 19 THEN '15 a 19 anos'
                    WHEN {idade_expr} BETWEEN 20 AND 29 THEN '20 a 29 anos'
                    WHEN {idade_expr} BETWEEN 30 AND 39 THEN '30 a 39 anos'
                    WHEN {idade_expr} BETWEEN 40 AND 49 THEN '40 a 49 anos'
                    WHEN {idade_expr} BETWEEN 50 AND 59 THEN '50 a 59 anos'
                    WHEN {idade_expr} BETWEEN 60 AND 69 THEN '60 a 69 anos'
                    WHEN {idade_expr} BETWEEN 70 AND 79 THEN '70 a 79 anos'
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
