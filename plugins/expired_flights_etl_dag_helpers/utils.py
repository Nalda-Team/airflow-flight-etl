# 공항 맵 로드
import json

AIRPORT_MAP_PATH = '/opt/airflow/plugins/maps/request_airport_map.json'
with open(AIRPORT_MAP_PATH, 'r', encoding='utf-8-sig') as f:
    request_airport_map = json.load(f)

def get_airport_lists():
    """출발 및 도착 공항 리스트 생성"""
    depart_airports = ['ICN', 'GMP']
    arrival_airports = ['CJU']
    
    for region in ['일본', '동남아', '중국', '유럽', '미주', '대양주', '남미', '중동']:
        for airport in request_airport_map[region]:
            arrival_airports.append(airport['IATA'])
            
    return depart_airports, arrival_airports