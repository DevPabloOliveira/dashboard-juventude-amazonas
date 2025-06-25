# preprocess.py - VERSÃO FINAL E CORRIGIDA

import json
import os
import pandas as pd
import geopandas as gpd

print("Iniciando o pré-processamento dos dados...")

# Caminhos
RAW_DATA_PATH = './data/juventude_amazonas_AM.geojson'
MAP_PATH = './data/br_geobr_mapas_municipio_am.geojson'
OUTPUT_DIR = './processed_data'

# Cria o diretório de saída se não existir
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Carrega os dados
gdf_data = gpd.read_file(RAW_DATA_PATH)
gdf_map = gpd.read_file(MAP_PATH)

# Normaliza nomes para o merge
gdf_data['NM_MUN_demanda'] = gdf_data['NM_MUN_demanda'].str.upper().str.strip()
gdf_map['nome_upper'] = gdf_map['nome'].str.upper().str.strip()

# --- LÓGICA DE AGREGAÇÃO COMPLETA ---
age_groups = ['geral', '15-19', '20-24', '25-29']
final_data = {}

for age_group in age_groups:
    print(f"Processando para a faixa etária: {age_group}...")
    
    # Mapeamentos explícitos para garantir que os nomes das colunas estejam corretos
    age_map = {
        '15-19': ['15-19 ANOS'], '20-24': ['20-24 ANOS'], '25-29': ['25-29 ANOS'],
        'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS']
    }
    
    # ===== AQUI ESTÁ A CORREÇÃO PRINCIPAL =====
    race_map = {
        '15-19': {'branca': ['15-19 ANOS, RAÇA BRANCA'], 'preta': ['15-19 ANOS, RAÇA PRETA'], 'parda': ['15-19 ANOS, RAÇA PARDA'], 'indigena': ['15-19 ANOS, RAÇA INDÍGENA'], 'amarela': ['15-19 ANOS, RAÇA AMARELA']},
        '20-24': {'branca': ['20-24 ANOS, RAÇA BRANCA'], 'preta': ['20-24 ANOS, RAÇA PRETA'], 'parda': ['20-24 ANOS, RAÇA PARDA'], 'indigena': ['20-24 ANOS, RAÇA INDÍGENA'], 'amarela': ['20-24 ANOS, RAÇA AMARELA']},
        '25-29': {'branca': ['25-29 ANOS, RAÇA BRANCA'], 'preta': ['25-29 ANOS, RAÇA PRETA'], 'parda': ['25-29 ANOS, RAÇA PARDA'], 'indigena': ['25-29 ANOS, RAÇA INDÍGENA'], 'amarela': ['25-29 ANOS, RAÇA AMARELA']},
        'geral': {
            'branca': ['15-19 ANOS, RAÇA BRANCA', '20-24 ANOS, RAÇA BRANCA', '25-29 ANOS, RAÇA BRANCA'],
            'preta': ['15-19 ANOS, RAÇA PRETA', '20-24 ANOS, RAÇA PRETA', '25-29 ANOS, RAÇA PRETA'],
            'parda': ['15-19 ANOS, RAÇA PARDA', '20-24 ANOS, RAÇA PARDA', '25-29 ANOS, RAÇA PARDA'],
            'indigena': ['15-19 ANOS, RAÇA INDÍGENA', '20-24 ANOS, RAÇA INDÍGENA', '25-29 ANOS, RAÇA INDÍGENA'],
            'amarela': ['15-19 ANOS, RAÇA AMARELA', '20-24 ANOS, RAÇA AMARELA', '25-29 ANOS, RAÇA AMARELA']
        }
    }

    literacy_map = {
        '15-19': ['15 A 19 ANOS, ALFABETIZADAS'], '20-24': ['20 A 24 ANOS, ALFABETIZADAS'], '25-29': ['25 A 29 ANOS, ALFABETIZADAS'],
        'geral': ['15 A 19 ANOS, ALFABETIZADAS', '20 A 24 ANOS, ALFABETIZADAS', '25 A 29 ANOS, ALFABETIZADAS']
    }
    
    age_cols = age_map[age_group]
    race_cols_map = race_map[age_group]
    literacy_cols = literacy_map[age_group]
    
    # Agregação por município
    all_cols_to_sum = age_cols + [item for sublist in race_cols_map.values() for item in sublist] + literacy_cols
    agg_functions = {col: 'sum' for col in all_cols_to_sum if col in gdf_data.columns}
    agg_functions['N_PESSOAS'] = 'sum'
    
    aggregated_df = gdf_data.groupby('NM_MUN_demanda').agg(agg_functions).reset_index()
    
    # Cálculos dinâmicos
    aggregated_df['total_jovens'] = aggregated_df[age_cols].sum(axis=1)
    aggregated_df['total_jovens_alfabetizados'] = aggregated_df[literacy_cols].sum(axis=1)
    aggregated_df['taxa_alfabetizacao_jovens'] = (aggregated_df['total_jovens_alfabetizados'] / aggregated_df['total_jovens']).fillna(0) * 100
    for race, cols in race_cols_map.items():
        existing_cols = [col for col in cols if col in aggregated_df.columns]
        aggregated_df[f'total_jovens_{race}'] = aggregated_df[existing_cols].sum(axis=1)

    # Renda e Vulnerabilidade
    df_vul_renda = gdf_data.copy()
    df_vul_renda['renda_ponderada'] = df_vul_renda['RENDA_MEDIA'].astype(float) * df_vul_renda['N_PESSOAS'].astype(float)
    df_vul_renda['vul_score_ponderado'] = df_vul_renda['VUL_SCORE'].astype(float) * df_vul_renda['N_PESSOAS'].astype(float)
    weighted_avg_df = df_vul_renda.groupby('NM_MUN_demanda')[['renda_ponderada', 'vul_score_ponderado', 'N_PESSOAS']].sum()
    weighted_avg_df['renda_media_final'] = weighted_avg_df['renda_ponderada'] / weighted_avg_df['N_PESSOAS']
    weighted_avg_df['vul_score_final'] = weighted_avg_df['vul_score_ponderado'] / weighted_avg_df['N_PESSOAS']
    aggregated_df = pd.merge(aggregated_df, weighted_avg_df[['renda_media_final', 'vul_score_final']], on='NM_MUN_demanda')
    
    final_data[age_group] = aggregated_df.to_dict(orient='records')

# --- SALVANDO OS ARQUIVOS PROCESSADOS ---

# 1. Dados dos Municípios
municipios_payload = {}
for age_group, records in final_data.items():
    municipios_payload[age_group] = {record['NM_MUN_demanda']: record for record in records}
with open(os.path.join(OUTPUT_DIR, 'municipios_data.json'), 'w', encoding='utf-8') as f:
    json.dump(municipios_payload, f, indent=2) # Adicionado indent para facilitar a leitura
print("Salvo: municipios_data.json")

# 2. Dados do Mapa
map_data_payload = {}
for age_group, records in final_data.items():
    df = pd.DataFrame(records)
    map_data_gdf = gdf_map.merge(df, left_on='nome_upper', right_on='NM_MUN_demanda', how='left')
    map_data_gdf = map_data_gdf.fillna(0)
    map_data_payload[age_group] = json.loads(map_data_gdf.to_json())
with open(os.path.join(OUTPUT_DIR, 'map_data.json'), 'w', encoding='utf-8') as f:
    json.dump(map_data_payload, f)
print("Salvo: map_data.json")

# 3. Lista de Municípios
municipio_list = sorted(gdf_data['NM_MUN_demanda'].unique().tolist())
with open(os.path.join(OUTPUT_DIR, 'municipio_list.json'), 'w', encoding='utf-8') as f:
    json.dump(municipio_list, f)
print("Salvo: municipio_list.json")

print("\nPré-processamento concluído com sucesso!")