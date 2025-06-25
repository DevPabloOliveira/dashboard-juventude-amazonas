# core/data_processor.py
import pandas as pd
import geopandas as gpd

DATA_PATH = './data/juventude_amazonas_AM.geojson'
MAP_PATH = './data/br_geobr_mapas_municipio_am.geojson'

class DataProcessor:
    def __init__(self):
        self.gdf_data = gpd.read_file(DATA_PATH)
        self.gdf_map = gpd.read_file(MAP_PATH)
        # Normaliza nomes de municípios uma única vez
        self.gdf_data['NM_MUN_demanda'] = self.gdf_data['NM_MUN_demanda'].str.upper().str.strip()
        self.gdf_map['nome_upper'] = self.gdf_map['nome'].str.upper().str.strip()

    def _get_dynamic_columns(self, age_group: str = 'geral'):
        # Retorna as colunas corretas com base na faixa etária selecionada
        age_map = {
            '15-19': ['15-19 ANOS'],
            '20-24': ['20-24 ANOS'],
            '25-29': ['25-29 ANOS'],
            'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS']
        }
        
        race_map = {
            '15-19': {'branca': ['15-19 ANOS, RAÇA BRANCA'], 'preta': ['15-19 ANOS, RAÇA PRETA'], 'parda': ['15-19 ANOS, RAÇA PARDA'], 'indigena': ['15-19 ANOS, RAÇA INDÍGENA'], 'amarela': ['15-19 ANOS, RAÇA AMARELA']},
            '20-24': {'branca': ['20-24 ANOS, RAÇA BRANCA'], 'preta': ['20-24 ANOS, RAÇA PRETA'], 'parda': ['20-24 ANOS, RAÇA PARDA'], 'indigena': ['20-24 ANOS, RAÇA INDÍGENA'], 'amarela': ['20-24 ANOS, RAÇA AMARELA']},
            '25-29': {'branca': ['25-29 ANOS, RAÇA BRANCA'], 'preta': ['25-29 ANOS, RAÇA PRETA'], 'parda': ['25-29 ANOS, RAÇA PARDA'], 'indigena': ['25-29 ANOS, RAÇA INDÍGENA'], 'amarela': ['25-29 ANOS, RAÇA AMARELA']},
            'geral': {'branca': ['15-19 ANOS, RAÇA BRANCA', '20-24 ANOS, RAÇA BRANCA', '25-29 ANOS, RAÇA BRANCA'], 'preta': ['15-19 ANOS, RAÇA PRETA', '20-24 ANOS, RAÇA PRETA', '25-29 ANOS, RAÇA PRETA'], 'parda': ['15-19 ANOS, RAÇA PARDA', '20-24 ANOS, RAÇA PARDA', '25-29 ANOS, RAÇA PARDA'], 'indigena': ['15-19 ANOS, RAÇA INDÍGENA', '20-24 ANOS, RAÇA INDÍGENA', '25-29 ANOS, RAÇA INDÍGENA'], 'amarela': ['15-19 ANOS, RAÇA AMARELA', '20-24 ANOS, RAÇA AMARELA', '25-29 ANOS, RAÇA AMARELA']}
        }

        literacy_map = {
            '15-19': ['15 A 19 ANOS, ALFABETIZADAS'],
            '20-24': ['20 A 24 ANOS, ALFABETIZADAS'],
            '25-29': ['25 A 29 ANOS, ALFABETIZADAS'],
            'geral': ['15 A 19 ANOS, ALFABETIZADAS', '20 A 24 ANOS, ALFABETIZADAS', '25 A 29 ANOS, ALFABETIZADAS']
        }
        
        return age_map[age_group], race_map[age_group], literacy_map[age_group]

    def _aggregate_data(self, age_group: str = 'geral'):
        # Função principal que calcula tudo dinamicamente
        age_cols, race_cols_map, literacy_cols = self._get_dynamic_columns(age_group)

        # Agrega as colunas relevantes
        all_cols_to_sum = age_cols + [item for sublist in race_cols_map.values() for item in sublist] + literacy_cols
        agg_functions = {col: 'sum' for col in all_cols_to_sum}
        agg_functions['N_PESSOAS'] = 'sum'
        
        aggregated_df = self.gdf_data.groupby('NM_MUN_demanda').agg(agg_functions).reset_index()

        # Calcula totais e taxas dinamicamente
        aggregated_df['total_jovens'] = aggregated_df[age_cols].sum(axis=1)
        aggregated_df['total_jovens_alfabetizados'] = aggregated_df[literacy_cols].sum(axis=1)
        aggregated_df['taxa_alfabetizacao_jovens'] = (aggregated_df['total_jovens_alfabetizados'] / aggregated_df['total_jovens']).fillna(0) * 100

        for race, cols in race_cols_map.items():
            aggregated_df[f'total_jovens_{race}'] = aggregated_df[cols].sum(axis=1)
            
        # Adiciona renda e vulnerabilidade (que não dependem da faixa etária da mesma forma)
        df_vul_renda = self.gdf_data.copy()
        df_vul_renda['renda_ponderada'] = df_vul_renda['RENDA_MEDIA'] * df_vul_renda['N_PESSOAS']
        df_vul_renda['vul_score_ponderado'] = df_vul_renda['VUL_SCORE'] * df_vul_renda['N_PESSOAS']
        
        weighted_avg_df = df_vul_renda.groupby('NM_MUN_demanda')[['renda_ponderada', 'vul_score_ponderado', 'N_PESSOAS']].sum()
        weighted_avg_df['renda_media_final'] = weighted_avg_df['renda_ponderada'] / weighted_avg_df['N_PESSOAS']
        weighted_avg_df['vul_score_final'] = weighted_avg_df['vul_score_ponderado'] / weighted_avg_df['N_PESSOAS']
        
        aggregated_df = pd.merge(aggregated_df, weighted_avg_df[['renda_media_final', 'vul_score_final']], on='NM_MUN_demanda')

        return aggregated_df

    def get_geral_data(self, age_group: str = 'geral'):
        aggregated_df = self._aggregate_data(age_group)
        geral = aggregated_df.sum(numeric_only=True)
        taxa_alfabetizacao_estado = (geral['total_jovens_alfabetizados'] / geral['total_jovens'] if geral['total_jovens'] > 0 else 0) * 100
        
        # O cálculo da renda média do estado é sobre toda a população, não apenas a agregada
        renda_media_estado = (self.gdf_data['RENDA_MEDIA'] * self.gdf_data['N_PESSOAS']).sum() / self.gdf_data['N_PESSOAS'].sum()
        
        age_cols, _, _ = self._get_dynamic_columns(age_group)
        
        return {
            "total_jovens": int(geral['total_jovens']),
            "renda_media": round(renda_media_estado, 2), # Renda média é geral do município/estado
            "taxa_alfabetizacao_jovens": round(taxa_alfabetizacao_estado, 2),
            "distribuicao_etaria": {col.replace(" ANOS", "").replace("-"," a "): int(geral[col]) for col in age_cols},
            "distribuicao_raca": {
                "branca": int(geral['total_jovens_branca']), "preta": int(geral['total_jovens_preta']),
                "parda": int(geral['total_jovens_parda']), "indigena": int(geral['total_jovens_indigena']),
                "amarela": int(geral['total_jovens_amarela']),
            }
        }
    
    def get_data_by_municipio(self, nome_municipio: str, age_group: str = 'geral'):
        aggregated_df = self._aggregate_data(age_group)
        data = aggregated_df[aggregated_df['NM_MUN_demanda'] == nome_municipio.upper()]
        if data.empty: return None
        
        municipio = data.iloc[0]
        age_cols, _, _ = self._get_dynamic_columns(age_group)
        
        return {
            "total_jovens": int(municipio['total_jovens']),
            "renda_media": round(municipio['renda_media_final'], 2),
            "taxa_alfabetizacao_jovens": round(municipio['taxa_alfabetizacao_jovens'], 2),
            "distribuicao_etaria": {col.replace(" ANOS", "").replace("-"," a "): int(municipio[col]) for col in age_cols},
            "distribuicao_raca": {
                "branca": int(municipio['total_jovens_branca']), "preta": int(municipio['total_jovens_preta']),
                "parda": int(municipio['total_jovens_parda']), "indigena": int(municipio['total_jovens_indigena']),
                "amarela": int(municipio['total_jovens_amarela']),
            }
        }

    def get_municipio_list(self):
        return sorted(self.gdf_data['NM_MUN_demanda'].unique().tolist())
        
    def get_map_data(self, age_group: str = 'geral'):
        aggregated_df = self._aggregate_data(age_group)
        map_data_gdf = self.gdf_map.merge(aggregated_df, left_on='nome_upper', right_on='NM_MUN_demanda', how='left')
        cols_to_keep = ['geometry', 'nome', 'total_jovens', 'renda_media_final', 'vul_score_final', 'taxa_alfabetizacao_jovens']
        return map_data_gdf[cols_to_keep].to_json()

    def get_ranking_by_metric(self, metric: str, age_group: str = 'geral', top_n: int = 5):
        aggregated_df = self._aggregate_data(age_group)
        metric_map = {'vulnerabilidade': 'vul_score_final', 'renda': 'renda_media_final', 'alfabetizacao': 'taxa_alfabetizacao_jovens', 'populacao': 'total_jovens'}
        column = metric_map.get(metric)
        if not column: return None

        ascending = True if metric == 'vulnerabilidade' else False
        sorted_df = aggregated_df.sort_values(by=column, ascending=ascending).fillna(0)
        
        df_result = sorted_df[['NM_MUN_demanda', column]].rename(columns={column: 'value', 'NM_MUN_demanda': 'municipio'})
        
        top_5 = df_result.head(top_n).to_dict(orient='records')
        bottom_5 = df_result.tail(top_n).sort_values(by='value', ascending=not ascending).to_dict(orient='records')

        return {"top_5": top_5, "bottom_5": bottom_5}

# Instância única
data_processor = DataProcessor()