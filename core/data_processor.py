import json
import os

# --- CAMINHOS PARA OS DADOS GRANULARES ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, 'processed_data')
DETAILS_DIR = os.path.join(PROCESSED_DATA_DIR, 'details')
RANKING_DIR = os.path.join(PROCESSED_DATA_DIR, 'rankings')
MAPS_DIR = os.path.join(PROCESSED_DATA_DIR, 'maps')

class DataProcessor:
    def __init__(self):
        try:
            with open(os.path.join(PROCESSED_DATA_DIR, 'municipio_list.json'), 'r', encoding='utf-8') as f:
                self.municipio_list = json.load(f)
        except FileNotFoundError:
            print(f"ERRO: Arquivo 'municipio_list.json' não encontrado em: {PROCESSED_DATA_DIR}")
            self.municipio_list = []

    def get_geral_data(self, age_group: str = 'geral'):
        file_path = os.path.join(DETAILS_DIR, 'AMAZONAS.json')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                all_state_data = json.load(f)
                return all_state_data.get(age_group)
        except FileNotFoundError:
            print(f"ERRO: Arquivo de dados gerais 'AMAZONAS.json' não encontrado.")
            return None

    def get_data_by_municipio(self, nome_municipio: str, age_group: str = 'geral'):
        """
        Busca os dados de um município e FORMATA a saída para ser consistente com a API.
        """
        municipio_name_upper = nome_municipio.upper()
        file_path = os.path.join(DETAILS_DIR, f'{municipio_name_upper}.json')
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                municipio_data_raw = json.load(f)
                data = municipio_data_raw.get(age_group)
                
                if not data:
                    return None

                # Formata o dicionário de saída para corresponder ao que o frontend espera,
                # garantindo consistência com a rota get_geral_data.
                age_cols_map = {'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS'], '15-19':['15-19 ANOS'], '20-24':['20-24 ANOS'], '25-29':['25-29 ANOS']}
                
                return {
                    "total_jovens": data.get('total_jovens', 0),
                    # Renomeia a chave 'renda_media_final' para 'renda_media'
                    "renda_media": data.get('renda_media_final', 0), 
                    "taxa_alfabetizacao_jovens": data.get('taxa_alfabetizacao_jovens', 0),
                    "distribuicao_etaria": {col.replace(" ANOS", "").replace("-", " a "): data.get(col, 0) for col in age_cols_map.get(age_group, [])},
                    "distribuicao_raca": {
                        "branca": data.get('total_jovens_branca', 0),
                        "preta": data.get('total_jovens_preta', 0),
                        "parda": data.get('total_jovens_parda', 0),
                        "indigena": data.get('total_jovens_indigena', 0),
                        "amarela": data.get('total_jovens_amarela', 0),
                    }
                }

        except FileNotFoundError:
            print(f"ERRO: Arquivo para o município '{municipio_name_upper}' não encontrado.")
            return None
        
    def get_municipio_list(self):
        return self.municipio_list

    def get_map_data(self, age_group: str = 'geral'):
        file_path = os.path.join(MAPS_DIR, f'map_data_{age_group}.json')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"ERRO: Arquivo de mapa 'map_data_{age_group}.json' não encontrado.")
            return {"type": "FeatureCollection", "features": []}

    def get_ranking_by_metric(self, metric: str, age_group: str = 'geral'):
        file_path = os.path.join(RANKING_DIR, f'ranking_{metric}_{age_group}.json')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"ERRO: Arquivo de ranking 'ranking_{metric}_{age_group}.json' não encontrado.")
            return {"top_5": [], "bottom_5": []}

data_processor = DataProcessor()