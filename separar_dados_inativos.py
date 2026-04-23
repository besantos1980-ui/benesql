import duckdb
import pandas as pd

# 1. Configuração inicial
arquivo_origem = "C:\inativos_ben\sib_inativo_*.csv"
arquivo_saida = "Relatorio_Beneficiarios_Trimestral.xlsx"

con = duckdb.connect(':memory:') # Para 50GB, o DuckDB gerencia o cache em disco automaticamente

# 2. Gerar lista de trimestres (Datas de corte)
# Usaremos o último dia de cada trimestre
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
        print(f"Processando {nome_aba} (Referência: {data_corte})...")
        
        # SQL robusto para processar 50GB colunarmente
        query = f"""
        WITH base_ativa AS (
            SELECT 
                REGISTRO_OPERADORA,
                CD_PLANO_RPS,
                CD_MUNICIPIO,
                TP_SEXO,
                -- Cálculo de idade na data de corte
                date_diff('year', CAST(DT_NASCIMENTO AS DATE), CAST('{data_corte}' AS DATE)) as idade
            FROM read_csv_auto('{arquivo_origem}')
            WHERE 
                -- Lógica de Ativo: Contratado antes da data E (não cancelado OU cancelado depois da data)
                CAST(DT_CONTRATACAO AS DATE) <= CAST('{data_corte}' AS DATE)
                AND (DT_CANCELAMENTO IS NULL OR CAST(DT_CANCELAMENTO AS DATE) > CAST('{data_corte}' AS DATE))
        ),
        faixas AS (
            SELECT *,
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
            FROM base_ativa
        )
        SELECT 
            REGISTRO_OPERADORA as Operadora,
            CD_PLANO_RPS as Produto,
            CD_MUNICIPIO as Municipio,
            TP_SEXO as Sexo,
            FAIXA_ETARIA as Faixa_Etaria,
            COUNT(*) as Total_Beneficiarios
        FROM faixas
        GROUP BY 1, 2, 3, 4, 5
        """
        
        df_trimestre = con.execute(query).df()
        df_trimestre.to_excel(writer, sheet_name=nome_aba, index=False)

print(f"Sucesso! Arquivo gerado: {arquivo_saida}")
