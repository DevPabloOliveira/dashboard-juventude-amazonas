import json
import os
import pandas as pd
import geopandas as gpd
from collections import defaultdict

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
        '15-19': { 'branca': ['15-19 ANOS, RAÇA BRANCA'], 'preta':  ['15-19 ANOS, RAÇA PRETA'], 'parda':  ['15-19 ANOS, RAÇA PARDA'], 'indigena': ['15-19 ANOS, RAÇA INDÍGENA'], 'amarela':  ['15-19 ANOS, RAÇA AMARELA'] },
        '20-24': { 'branca': ['20-24 ANOS, RAÇA BRANCA'], 'preta':  ['20-24 ANOS, RAÇA PRETA'], 'parda':  ['20-24 ANOS, RAÇA PARDA'], 'indigena': ['20-24 ANOS, RAÇA INDÍGENA'], 'amarela':  ['20-24 ANOS, RAÇA AMARELA'] },
        '25-29': { 'branca': ['25-29 ANOS, RAÇA BRANCA'], 'preta':  ['25-29 ANOS, RAÇA PRETA'], 'parda':  ['25-29 ANOS, RAÇA PARDA'], 'indigena': ['25-29 ANOS, RAÇA INDÍGENA'], 'amarela':  ['25-29 ANOS, RAÇA AMARELA'] },
        'geral': { 'branca': ['15-19 ANOS, RAÇA BRANCA', '20-24 ANOS, RAÇA BRANCA', '25-29 ANOS, RAÇA BRANCA'], 'preta':  ['15-19 ANOS, RAÇA PRETA',  '20-24 ANOS, RAÇA PRETA',  '25-29 ANOS, RAÇA PRETA'], 'parda':  ['15-19 ANOS, RAÇA PARDA',  '20-24 ANOS, RAÇA PARDA',  '25-29 ANOS, RAÇA PARDA'], 'indigena': ['15-19 ANOS, RAÇA INDÍGENA', '20-24 ANOS, RAÇA INDÍGENA', '25-29 ANOS, RAÇA INDÍGENA'], 'amarela':  ['15-19 ANOS, RAÇA AMARELA',  '20-24 ANOS, RAÇA AMARELA',  '25-29 ANOS, RAÇA AMARELA'] }
    }
    race_cols_map = race_map[age_group]

    # Colunas de alfabetização por faixa etária
    literacy_map = {
        '15-19': ['15 A 19 ANOS, ALFABETIZADAS'], '20-24': ['20 A 24 ANOS, ALFABETIZADAS'], '25-29': ['25 A 29 ANOS, ALFABETIZADAS'],
        'geral': ['15 A 19 ANOS, ALFABETIZADAS', '20 A 24 ANOS, ALFABETIZADAS', '25 A 29 ANOS, ALFABETIZADAS']
    }
    literacy_cols = literacy_map[age_group]

    # Preparação de funções de agregação
    cols_to_sum = age_cols + sum(race_cols_map.values(), []) + literacy_cols
    agg_funcs = {col: 'sum' for col in cols_to_sum if col in gdf_data.columns}

    # Agrupamento por município
    aggregated_df = gdf_data.groupby('NM_MUN_demanda').agg(agg_funcs).reset_index()

    # Cálculo de totais e taxas
    aggregated_df['total_jovens'] = aggregated_df[age_cols].sum(axis=1)
    aggregated_df['total_jovens_alfabetizados'] = aggregated_df[literacy_cols].sum(axis=1)
    
    aggregated_df['taxa_alfabetizacao_jovens'] = 0.0
    mask = aggregated_df['total_jovens'] > 0
    aggregated_df.loc[mask, 'taxa_alfabetizacao_jovens'] = (aggregated_df.loc[mask, 'total_jovens_alfabetizados'] / aggregated_df.loc[mask, 'total_jovens'] * 100)

    for race, cols in race_cols_map.items():
        valid_cols = [c for c in cols if c in aggregated_df.columns]
        aggregated_df[f'total_jovens_{race}'] = aggregated_df[valid_cols].sum(axis=1)

    # Cálculo de médias ponderadas (feito apenas uma vez para otimizar)
    if 'renda_media_final' not in aggregated_df.columns:
        tmp = gdf_data.copy()
        tmp['renda_ponderada'] = tmp['RENDA_MEDIA'].astype(float) * tmp['N_PESSOAS'].astype(float)
        tmp['vul_score_ponderado'] = tmp['VUL_SCORE'].astype(float) * tmp['N_PESSOAS'].astype(float)
        weighted = tmp.groupby('NM_MUN_demanda')[['renda_ponderada', 'vul_score_ponderado', 'N_PESSOAS']].sum()
        
        weighted['renda_media_final'] = 0.0
        weighted['vul_score_final'] = 0.0
        mask_w = weighted['N_PESSOAS'] > 0
        weighted.loc[mask_w, 'renda_media_final'] = weighted['renda_ponderada'] / weighted['N_PESSOAS']
        weighted.loc[mask_w, 'vul_score_final']   = weighted['vul_score_ponderado'] / weighted['N_PESSOAS']
        
        n_pessoas_df = tmp.groupby('NM_MUN_demanda')['N_PESSOAS'].sum().reset_index()
        aggregated_df = aggregated_df.merge(n_pessoas_df, on='NM_MUN_demanda')
        aggregated_df = aggregated_df.merge(weighted[['renda_media_final', 'vul_score_final']], on='NM_MUN_demanda')

    final_data[age_group] = aggregated_df.to_dict(orient='records')

from collections import defaultdict

data_by_municipality = defaultdict(dict)
for age_group, records in final_data.items():
    for record in records:
        municipio_name = record['NM_MUN_demanda']
        data_by_municipality[municipio_name][age_group] = record

DETAILS_DIR = os.path.join(OUTPUT_DIR, 'details')
os.makedirs(DETAILS_DIR, exist_ok=True)

for municipio_name, age_data in data_by_municipality.items():
    file_path = os.path.join(DETAILS_DIR, f'{municipio_name}.json')
    with open(file_path, 'w', encoding='utf-8') as f: json.dump(age_data, f, indent=2)
print(f"Salvo: Dados detalhados para {len(data_by_municipality)} municípios.")

# Calcular e salvar dados do estado
estado_data = {}
age_cols_map = {'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS'], '15-19':['15-19 ANOS'], '20-24':['20-24 ANOS'], '25-29':['25-29 ANOS']}

for age_group, records in final_data.items():
    df = pd.DataFrame(records)
    estado_geral = {col: df[col].sum() for col in df.select_dtypes(include='number').columns}
    
    total_jovens_estado = estado_geral.get('total_jovens', 0)
    estado_geral['taxa_alfabetizacao_jovens'] = (estado_geral.get('total_jovens_alfabetizados', 0) / total_jovens_estado * 100) if total_jovens_estado > 0 else 0
    
    total_pessoas_estado = df['N_PESSOAS'].sum()
    estado_geral['renda_media_final'] = ((df['renda_media_final'] * df['N_PESSOAS']).sum() / total_pessoas_estado) if total_pessoas_estado > 0 else 0
        
    estado_data[age_group] = {
        "total_jovens": int(total_jovens_estado), "renda_media": round(estado_geral.get('renda_media_final', 0), 2),
        "taxa_alfabetizacao_jovens": round(estado_geral.get('taxa_alfabetizacao_jovens', 0), 2),
        "distribuicao_etaria": {col.replace(" ANOS", "").replace("-", " a "): int(estado_geral.get(col, 0)) for col in age_cols_map.get(age_group, [])},
        "distribuicao_raca": { "branca": int(estado_geral.get('total_jovens_branca', 0)), "preta": int(estado_geral.get('total_jovens_preta', 0)), "parda": int(estado_geral.get('total_jovens_parda', 0)), "indigena": int(estado_geral.get('total_jovens_indigena', 0)), "amarela": int(estado_geral.get('total_jovens_amarela', 0)), }
    }

with open(os.path.join(DETAILS_DIR, 'AMAZONAS.json'), 'w', encoding='utf-8') as f: json.dump(estado_data, f, indent=2)
print("Salvo: Dados agregados para o estado 'AMAZONAS'.")

# Pré-calcular e salvar Rankings
RANKING_DIR = os.path.join(OUTPUT_DIR, 'rankings')
os.makedirs(RANKING_DIR, exist_ok=True)
metric_map = {'vulnerabilidade': 'vul_score_final', 'renda': 'renda_media_final', 'alfabetizacao': 'taxa_alfabetizacao_jovens', 'populacao': 'total_jovens'}

for age_group, records in final_data.items():
    df = pd.DataFrame(records)
    for metric_name, column in metric_map.items():
        ascending = True if metric_name == 'vulnerabilidade' else False
        sorted_df = df.sort_values(by=column, ascending=ascending)
        top_5 = sorted_df.head(5)[['NM_MUN_demanda', column]].rename(columns={'NM_MUN_demanda': 'municipio', column: 'value'})
        bottom_5 = sorted_df.tail(5)[['NM_MUN_demanda', column]].rename(columns={'NM_MUN_demanda': 'municipio', column: 'value'})
        bottom_5 = bottom_5.sort_values(by='value', ascending=not ascending)
        ranking_data = { "top_5": top_5.to_dict(orient='records'), "bottom_5": bottom_5.to_dict(orient='records') }
        with open(os.path.join(RANKING_DIR, f'ranking_{metric_name}_{age_group}.json'), 'w', encoding='utf-8') as f: json.dump(ranking_data, f, indent=2)
print("Salvo: Rankings pré-calculados.")

# Salvar dados de Mapa por faixa etária
MAPS_DIR = os.path.join(OUTPUT_DIR, 'maps')
os.makedirs(MAPS_DIR, exist_ok=True)
for age_group, records in final_data.items():
    df = pd.DataFrame(records)
    merged_map = gdf_map.merge(df, left_on='nome_upper', right_on='NM_MUN_demanda', how='left').fillna(0)
    with open(os.path.join(MAPS_DIR, f'map_data_{age_group}.json'), 'w', encoding='utf-8') as f: json.dump(json.loads(merged_map.to_json()), f)
print("Salvo: Dados de mapa por faixa etária.")

# Salvar lista de municípios
municipio_list = sorted(gdf_data['NM_MUN_demanda'].unique())
with open(os.path.join(OUTPUT_DIR, 'municipio_list.json'), 'w', encoding='utf-8') as f: json.dump(municipio_list, f, indent=2)
print("Salvo: municipio_list.json")

print("Pré-processamento concluído com sucesso!")