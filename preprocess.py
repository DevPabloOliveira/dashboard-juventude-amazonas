import json
import os
import pandas as pd
import geopandas as gpd

print("Iniciando pré-processamento dos dados...")

# Configuração de caminhos de entrada e saída
RAW_DATA_PATH = './data/juventude_amazonas_AM.geojson'
MAP_PATH      = './data/br_geobr_mapas_municipio_am.geojson'
OUTPUT_DIR    = './processed_data'

# Garante existência da pasta de saída
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Carregamento dos dados geoespaciais
gdf_data = gpd.read_file(RAW_DATA_PATH)
gdf_map  = gpd.read_file(MAP_PATH)

# Normalização de nomes de município para fusão
gdf_data['NM_MUN_demanda'] = (
    gdf_data['NM_MUN_demanda']
    .str.upper()
    .str.strip()
)
gdf_map['nome_upper'] = (
    gdf_map['nome']
    .str.upper()
    .str.strip()
)

# Lista de faixas etárias a processar
age_groups = ['geral', '15-19', '20-24', '25-29']
final_data = {}

for age_group in age_groups:
    print(f"Processando faixa etária: {age_group}")

    # Colunas de contagem por faixa etária
    age_map = {
        '15-19': ['15-19 ANOS'],
        '20-24': ['20-24 ANOS'],
        '25-29': ['25-29 ANOS'],
        'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS']
    }
    age_cols = age_map[age_group]

    # Colunas de raça por faixa etária
    race_map = {
        '15-19': {
            'branca': ['15-19 ANOS, RAÇA BRANCA'],
            'preta':  ['15-19 ANOS, RAÇA PRETA'],
            'parda':  ['15-19 ANOS, RAÇA PARDA'],
            'indigena': ['15-19 ANOS, RAÇA INDÍGENA'],
            'amarela':  ['15-19 ANOS, RAÇA AMARELA']
        },
        '20-24': {
            'branca': ['20-24 ANOS, RAÇA BRANCA'],
            'preta':  ['20-24 ANOS, RAÇA PRETA'],
            'parda':  ['20-24 ANOS, RAÇA PARDA'],
            'indigena': ['20-24 ANOS, RAÇA INDÍGENA'],
            'amarela':  ['20-24 ANOS, RAÇA AMARELA']
        },
        '25-29': {
            'branca': ['25-29 ANOS, RAÇA BRANCA'],
            'preta':  ['25-29 ANOS, RAÇA PRETA'],
            'parda':  ['25-29 ANOS, RAÇA PARDA'],
            'indigena': ['25-29 ANOS, RAÇA INDÍGENA'],
            'amarela':  ['25-29 ANOS, RAÇA AMARELA']
        },
        'geral': {
            'branca': ['15-19 ANOS, RAÇA BRANCA', '20-24 ANOS, RAÇA BRANCA', '25-29 ANOS, RAÇA BRANCA'],
            'preta':  ['15-19 ANOS, RAÇA PRETA',  '20-24 ANOS, RAÇA PRETA',  '25-29 ANOS, RAÇA PRETA'],
            'parda':  ['15-19 ANOS, RAÇA PARDA',  '20-24 ANOS, RAÇA PARDA',  '25-29 ANOS, RAÇA PARDA'],
            'indigena': ['15-19 ANOS, RAÇA INDÍGENA', '20-24 ANOS, RAÇA INDÍGENA', '25-29 ANOS, RAÇA INDÍGENA'],
            'amarela':  ['15-19 ANOS, RAÇA AMARELA',  '20-24 ANOS, RAÇA AMARELA',  '25-29 ANOS, RAÇA AMARELA']
        }
    }
    race_cols_map = race_map[age_group]

    # Colunas de alfabetização por faixa etária
    literacy_map = {
        '15-19': ['15 A 19 ANOS, ALFABETIZADAS'],
        '20-24': ['20 A 24 ANOS, ALFABETIZADAS'],
        '25-29': ['25 A 29 ANOS, ALFABETIZADAS'],
        'geral': ['15 A 19 ANOS, ALFABETIZADAS', '20 A 24 ANOS, ALFABETIZADAS', '25 A 29 ANOS, ALFABETIZADAS']
    }
    literacy_cols = literacy_map[age_group]

    # Preparação de funções de agregação
    cols_to_sum = age_cols + sum(race_cols_map.values(), []) + literacy_cols + ['N_PESSOAS']
    agg_funcs = {col: 'sum' for col in cols_to_sum if col in gdf_data.columns}

    # Agrupamento por município
    aggregated_df = (
        gdf_data
        .groupby('NM_MUN_demanda')
        .agg(agg_funcs)
        .reset_index()
    )

    # Cálculo de totais e taxas
    aggregated_df['total_jovens'] = aggregated_df[age_cols].sum(axis=1)
    aggregated_df['total_jovens_alfabetizados'] = aggregated_df[literacy_cols].sum(axis=1)
    aggregated_df['taxa_alfabetizacao_jovens'] = (
        aggregated_df['total_jovens_alfabetizados'] /
        aggregated_df['total_jovens']
    ).fillna(0) * 100

    for race, cols in race_cols_map.items():
        valid_cols = [c for c in cols if c in aggregated_df.columns]
        aggregated_df[f'total_jovens_{race}'] = aggregated_df[valid_cols].sum(axis=1)

    # Cálculo de médias ponderadas de renda e vulnerabilidade
    tmp = gdf_data.copy()
    tmp['renda_ponderada']     = tmp['RENDA_MEDIA'].astype(float) * tmp['N_PESSOAS'].astype(float)
    tmp['vul_score_ponderado'] = tmp['VUL_SCORE'].astype(float)   * tmp['N_PESSOAS'].astype(float)

    weighted = (
        tmp
        .groupby('NM_MUN_demanda')[['renda_ponderada', 'vul_score_ponderado', 'N_PESSOAS']]
        .sum()
    )
    weighted['renda_media_final'] = weighted['renda_ponderada'] / weighted['N_PESSOAS']
    weighted['vul_score_final']   = weighted['vul_score_ponderado'] / weighted['N_PESSOAS']

    aggregated_df = aggregated_df.merge(
        weighted[['renda_media_final', 'vul_score_final']],
        on='NM_MUN_demanda'
    )

    final_data[age_group] = aggregated_df.to_dict(orient='records')

# Exportação dos resultados processados

# 1. Dados por município e faixa etária
municipios_payload = {
    age: {row['NM_MUN_demanda']: row for row in records}
    for age, records in final_data.items()
}
with open(os.path.join(OUTPUT_DIR, 'municipios_data.json'), 'w', encoding='utf-8') as f:
    json.dump(municipios_payload, f, indent=2)
print("Salvo: municipios_data.json")

# 2. Dados para o componente de mapa
map_data_payload = {}
for age, records in final_data.items():
    df = pd.DataFrame(records)
    merged_map = (
        gdf_map
        .merge(df, left_on='nome_upper', right_on='NM_MUN_demanda', how='left')
        .fillna(0)
    )
    map_data_payload[age] = json.loads(merged_map.to_json())
with open(os.path.join(OUTPUT_DIR, 'map_data.json'), 'w', encoding='utf-8') as f:
    json.dump(map_data_payload, f, indent=2)
print("Salvo: map_data.json")

# 3. Lista de municípios
municipio_list = sorted(gdf_data['NM_MUN_demanda'].unique())
with open(os.path.join(OUTPUT_DIR, 'municipio_list.json'), 'w', encoding='utf-8') as f:
    json.dump(municipio_list, f, indent=2)
print("Salvo: municipio_list.json")

print("Pré-processamento concluído.")
