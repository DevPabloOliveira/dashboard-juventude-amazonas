import json
import os


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Constrói o caminho absoluto para a pasta de dados processados
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, 'processed_data')


class DataProcessor:
    def __init__(self):
        # Carrega os dados pré-processados usando o caminho absoluto
        try:
            with open(os.path.join(PROCESSED_DATA_DIR, 'municipios_data.json'), 'r', encoding='utf-8') as f:
                self.municipios_data = json.load(f)
            with open(os.path.join(PROCESSED_DATA_DIR, 'map_data.json'), 'r', encoding='utf-8') as f:
                self.map_data = json.load(f)
            with open(os.path.join(PROCESSED_DATA_DIR, 'municipio_list.json'), 'r', encoding='utf-8') as f:
                self.municipio_list = json.load(f)
        except FileNotFoundError:
            print(f"ERRO: Arquivos de dados não encontrados no diretório: {PROCESSED_DATA_DIR}")
            print("Certifique-se de que o script 'preprocess.py' foi executado.")
            self.municipios_data = {}
            self.map_data = {}
            self.municipio_list = []

    # --- O restante do arquivo não precisa de nenhuma alteração ---

    def get_geral_data(self, age_group: str = 'geral'):
        all_mun_data = self.municipios_data.get(age_group, {}).values()
        if not all_mun_data: return {}

        geral = {}
        for key in next(iter(all_mun_data), {}):
            if isinstance(next(iter(all_mun_data), {})[key], (int, float)):
                geral[key] = sum(mun.get(key, 0) for mun in all_mun_data)

        total_jovens = geral.get('total_jovens', 0)
        taxa_alfabetizacao = (geral.get('total_jovens_alfabetizados', 0) / total_jovens if total_jovens > 0 else 0) * 100
        
        renda_media_total = sum(mun.get('renda_media_final', 0) for mun in all_mun_data)
        renda_media_estado = renda_media_total / len(all_mun_data) if all_mun_data else 0
        
        age_cols_map = {'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS'], '15-19':['15-19 ANOS'], '20-24':['20-24 ANOS'], '25-29':['25-29 ANOS']}
        
        return {
            "total_jovens": int(total_jovens),
            "renda_media": round(renda_media_estado, 2),
            "taxa_alfabetizacao_jovens": round(taxa_alfabetizacao, 2),
            "distribuicao_etaria": {col.replace(" ANOS", "").replace("-", " a "): int(geral.get(col, 0)) for col in age_cols_map.get(age_group, [])},
            "distribuicao_raca": {
                "branca": int(geral.get('total_jovens_branca', 0)), "preta": int(geral.get('total_jovens_preta', 0)),
                "parda": int(geral.get('total_jovens_parda', 0)), "indigena": int(geral.get('total_jovens_indigena', 0)),
                "amarela": int(geral.get('total_jovens_amarela', 0)),
            }
        }

    def get_data_by_municipio(self, nome_municipio: str, age_group: str = 'geral'):
        mun_data = self.municipios_data.get(age_group, {}).get(nome_municipio.upper())
        if not mun_data: return None

        age_cols_map = {'geral': ['15-19 ANOS', '20-24 ANOS', '25-29 ANOS'], '15-19':['15-19 ANOS'], '20-24':['20-24 ANOS'], '25-29':['25-29 ANOS']}

        return {
            "total_jovens": mun_data.get('total_jovens', 0),
            "renda_media": mun_data.get('renda_media_final', 0),
            "taxa_alfabetizacao_jovens": mun_data.get('taxa_alfabetizacao_jovens', 0),
            "distribuicao_etaria": {col.replace(" ANOS", "").replace("-", " a "): mun_data.get(col, 0) for col in age_cols_map.get(age_group, [])},
            "distribuicao_raca": {
                "branca": mun_data.get('total_jovens_branca', 0), "preta": mun_data.get('total_jovens_preta', 0),
                "parda": mun_data.get('total_jovens_parda', 0), "indigena": mun_data.get('total_jovens_indigena', 0),
                "amarela": mun_data.get('total_jovens_amarela', 0),
            }
        }
        
    def get_municipio_list(self):
        return self.municipio_list

    def get_map_data(self, age_group: str = 'geral'):
        return self.map_data.get(age_group, {"type": "FeatureCollection", "features": []})

    def get_ranking_by_metric(self, metric: str, age_group: str = 'geral', top_n: int = 5):
        metric_map = {'vulnerabilidade': 'vul_score_final', 'renda': 'renda_media_final', 'alfabetizacao': 'taxa_alfabetizacao_jovens', 'populacao': 'total_jovens'}
        column = metric_map.get(metric)
        if not column: return None

        all_mun_data = list(self.municipios_data.get(age_group, {}).values())
        if not all_mun_data: return {"top_5": [], "bottom_5": []}

        ascending = True if metric == 'vulnerabilidade' else False
        
        valid_data = [d for d in all_mun_data if d.get(column) is not None and d.get(column) > 0]
        
        sorted_data = sorted(valid_data, key=lambda x: x.get(column, 0), reverse=not ascending)
        
        top_5 = [{"municipio": item['NM_MUN_demanda'], "value": item[column]} for item in sorted_data[:top_n]]
        bottom_5 = [{"municipio": item['NM_MUN_demanda'], "value": item[column]} for item in sorted_data[-top_n:]]
        
        if not ascending:
            bottom_5.sort(key=lambda x: x['value'], reverse=False)
        else:
            bottom_5.sort(key=lambda x: x['value'], reverse=True)

        return {"top_5": top_5, "bottom_5": bottom_5}


data_processor = DataProcessor()