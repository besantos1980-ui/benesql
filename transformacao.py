import pandas as pd
import os
import glob

# ==============================================================================
# 1. CONFIGURAÇÕES DE CAMINHOS
# ==============================================================================
pasta_input = r"C:\inativos_ben\saida_trimestres"
pasta_output = r"C:\inativos_ben\bases_pivotadas"

if not os.path.exists(pasta_output):
    os.makedirs(pasta_output)

# Ordem exata das colunas conforme o padrão ANS e seu anexo
ordem_faixas = [
    '<1 ano', '1 a 4 anos', '5 a 9 anos', '10 a 14 anos', '15 a 19 anos',
    '20 a 29 anos', '30 a 39 anos', '40 a 49 anos', '50 a 59 anos',
    '60 a 69 anos', '70 a 79 anos', '80 anos ou mais', 'Idade Desconhecida'
]

print("--- Iniciando Transformação para Formato Pivot (Excel) ---")

# 2. BUSCA TODOS OS ARQUIVOS GERADOS NO PASSO ANTERIOR
arquivos = glob.glob(os.path.join(pasta_input, "ativos_*.csv"))

if not arquivos:
    print("Nenhum arquivo 'ativos_*.csv' encontrado na pasta de entrada.")
else:
    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        nome_base = nome_arquivo.replace(".csv", "")
        print(f"Processando: {nome_arquivo}...")

        # Lê o CSV (usando o delimitador ';' definido no script anterior)
        df = pd.read_csv(arquivo, sep=';')

        # 3. CRIAÇÃO DA TABELA DINÂMICA (PIVOT)
        # Linhas: Produto
        # Colunas: Faixa_Etaria
        # Valores: Total
        
        # Identifica a coluna de contagem (no script anterior chamamos de 'Total')
        col_valor = 'Total' if 'Total' in df.columns else 'Total_Beneficiarios'

        pivot_df = pd.pivot_table(
            df, 
            values=col_valor, 
            index=['Produto'], 
            columns=['Faixa_Etaria'], 
            aggfunc='sum',
            fill_value=0
        )

        # 4. PADRONIZAÇÃO DAS COLUNAS
        # Garante que todas as faixas etárias existam como colunas, mesmo que vazias
        for faixa in ordem_faixas:
            if faixa not in pivot_df.columns:
                pivot_df[faixa] = 0
        
        # Reordena as colunas para o padrão correto
        pivot_df = pivot_df[ordem_faixas]

        # Adiciona a coluna de Total Geral (Soma da linha)
        pivot_df['Total Geral'] = pivot_df.sum(axis=1)

        # 5. EXPORTAÇÃO PARA EXCEL
        # Salva cada trimestre em um arquivo Excel individual conforme solicitado
        caminho_excel = os.path.join(pasta_output, f"Base_Final_{nome_base}.xlsx")
        
        # Usamos o index_label para que a coluna de produto tenha nome no Excel
        pivot_df.to_excel(caminho_excel, index_label='Rótulos de Linha')

    print(f"\n--- SUCESSO! ---")
    print(f"As bases formatadas foram geradas em: {pasta_output}")
