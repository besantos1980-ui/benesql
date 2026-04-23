import duckdb
con = duckdb.connect('processamento_ans.db')

print("--- Amostra de Dados Higienizados ---")
df_debug = con.execute("""
    SELECT 
        DT_NASCIMENTO, 
        DT_CONTRATACAO, 
        DT_CANCELAMENTO,
        typeof(DT_CONTRATACAO) as tipo_contratacao
    FROM base_limpa 
    LIMIT 5
""").df()

print(df_debug)

check = con.execute("SELECT COUNT(*) FROM base_limpa WHERE DT_CONTRATACAO IS NOT NULL").fetchone()
print(f"\nLinhas com data de contratação válida: {check[0]}")
