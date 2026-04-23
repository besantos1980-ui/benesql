import duckdb
import pandas as pd

arquivo_origem = r"C:\inativos_ben\sib_inativo_SP.csv"
arquivo_saida = "Relatorio_Beneficiarios_Trimestral.xlsx"

# Conectando...
con = duckdb.connect('cache_processamento.db')

print("--- Passo 1: Lendo e Higienizando Dados (Modo Robusto) ---")

con.execute(f"""
    CREATE OR REPLACE TABLE base_limpa AS 
    SELECT 
        REGISTRO_OPERADORA,
        CD_PLANO_RPS,
        CD_MUNICIPIO,
        TP_SEXO,
        
        -- Lógica para DT_NASCIMENTO (A mais problemática)
        CASE 
            WHEN DT_NASCIMENTO ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN strptime(DT_NASCIMENTO, '%d/%m/%Y')::DATE
            WHEN DT_NASCIMENTO ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' THEN DT_NASCIMENTO::DATE
            WHEN DT_NASCIMENTO ~ '^\d{{4}}$' THEN (DT_NASCIMENTO || '-01-01')::DATE
            ELSE try_cast(DT_NASCIMENTO AS DATE)
        END as DT_NASCIMENTO,

        -- Lógica para DT_CONTRATACAO
        CASE 
            WHEN DT_CONTRATACAO ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN strptime(DT_CONTRATACAO, '%d/%m/%Y')::DATE
            WHEN DT_CONTRATACAO ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' THEN DT_CONTRATACAO::DATE
            ELSE try_cast(DT_CONTRATACAO AS DATE)
        END as DT_CONTRATACAO,

        -- Lógica para DT_CANCELAMENTO
        CASE 
            WHEN DT_CANCELAMENTO ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN strptime(DT_CANCELAMENTO, '%d/%m/%Y')::DATE
            WHEN DT_CANCELAMENTO ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' THEN DT_CANCELAMENTO::DATE
            ELSE try_cast(DT_CANCELAMENTO AS DATE)
        END as DT_CANCELAMENTO

    FROM read_csv('{arquivo_origem}', 
                  delim=';', 
                  header=True, 
                  all_varchar=True)
""")
# TESTE DE DIAGNÓSTICO: Vamos ver se a tabela tem dados
check = con.execute("SELECT COUNT(*), COUNT(DT_CONTRATACAO) FROM base_limpa").fetchone()
print(f"Total de linhas lidas: {check[0]}")
print(f"Linhas com data de contratação válida: {check[1]}")

if check[1] == 0:
    print("⚠️ ALERTA: As datas não foram convertidas corretamente. Verifique o delimitador ou o formato das datas no CSV.")
else:
    print("--- Passo 2: Gerando Abas (Apenas se houver dados) ---")
    
    trimestres = [('1T2018', '2018-03-31'), ('2T2024', '2024-06-30')] # Simplifiquei para teste

    with pd.ExcelWriter(arquivo_saida, engine='xlsxwriter') as writer:
        for nome_aba, data_corte in trimestres:
            query = f"""
            SELECT 
                REGISTRO_OPERADORA as Operadora,
                CD_PLANO_RPS as Produto,
                CD_MUNICIPIO as Municipio,
                TP_SEXO as Sexo,
                CASE 
                    WHEN date_diff('year', DT_NASCIMENTO, CAST('{data_corte}' AS DATE)) < 1 THEN '<1 ano'
                    -- ... (as outras faixas etárias entram aqui)
                    ELSE '80 anos ou mais'
                END as Faixa_Etaria,
                COUNT(*) as Total_Beneficiarios
            FROM base_limpa
            WHERE DT_CONTRATACAO <= '{data_corte}'
              AND (DT_CANCELAMENTO IS NULL OR DT_CANCELAMENTO > '{data_corte}')
            GROUP BY 1, 2, 3, 4, 5
            """
            
            df = con.execute(query).df()
            print(f"Aba {nome_aba}: {len(df)} linhas encontradas.")
            
            if not df.empty:
                df.to_excel(writer, sheet_name=nome_aba, index=False)
            else:
                # Cria uma aba de aviso se estiver vazio
                pd.DataFrame({"Aviso": ["Nenhum dado encontrado para este critério"]}).to_excel(writer, sheet_name=nome_aba)

    print(f"Processo finalizado. Verifique o arquivo: {arquivo_saida}")
