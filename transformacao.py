import pandas as pd
import os
import glob

pasta_input = r"C:\inativos_ben\saida_trimestres"
pasta_output = r"C:\inativos_ben\bases_pivotadas"
if not os.path.exists(pasta_output): os.makedirs(pasta_output)

ordem_faixas = [
    '<1 ano', '1 a 4 anos', '5 a 9 anos', '10 a 14 anos', '15 a 19 anos',
    '20 a 29 anos', '30 a 39 anos', '40 a 49 anos', '50 a 59 anos',
    '60 a 69 anos', '70 a 79 anos', '80 anos ou mais', 'Idade Desconhecida'
]

arquivos = glob.glob(os.path.join(pasta_input, "ativos_*.csv"))

for arquivo in arquivos:
    nome_base = os.path.basename(arquivo).replace(".csv", "")
    print(f"Pivotando: {nome_base}...")
    
    df = pd.read_csv(arquivo, sep=';')
    
    # GARANTIA: Trata nulos no Produto que possam ter passado
    df['Produto'] = df['Produto'].fillna('NÃO IDENTIFICADO').astype(str)
    
    # Pivotagem
    pivot_df = pd.pivot_table(
        df, values='Total', index=['Produto'], columns=['Faixa_Etaria'], 
        aggfunc='sum', fill_value=0
    )
    
    # Reindexar colunas para garantir que todas as faixas (incluindo 80+) apareçam
    pivot_df = pivot_df.reindex(columns=ordem_faixas, fill_value=0)
    
    # Total Geral
    pivot_df['Total Geral'] = pivot_df.sum(axis=1)
    
    pivot_df.to_excel(os.path.join(pasta_output, f"Final_{nome_base}.xlsx"), index_label='Produto')

print("Processo concluído.")
