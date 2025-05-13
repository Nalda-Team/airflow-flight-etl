import json
import pandas as pd
from datetime import timedelta

def get_weekday_kr(date):
    """날짜를 한글 요일로 변환"""
    weekdays = ['월', '화', '수', '목', '금', '토', '일']
    return weekdays[date.weekday()]

class FlightDataFormatter:
    """항공권 데이터 전처리 및 포맷팅"""
    
    def __init__(self, min_data_points: int = 3):
        self.min_data_points = min_data_points
    
    @classmethod 
    def process_dataframe(cls, df):
        # SQL 결과와 매핑할 컬럼명 
        column_mapping = {
            'air_id': 'air_id',
            'is_layover': 'is_layover', 
            'airport_code': 'airport_code(dep)',
            'departure_airport': 'depart_airport',
            'departure_country': 'depart_country',
            'airport_code_1': 'airport_code(arr)', 
            'arrival_airport': 'arrival_airport',
            'arrival_country': 'arrival_country',
            'airline': 'airline',
            'depart_timestamp': 'depart_time(utc)',
            'arrival_timestamp': 'arrival_time(utc)', 
            'depart_time_in_dep': 'depart_time(dep)',
            'arrival_time_in_dep': 'arrival_time(dep)',
            'depart_time_in_arr': 'depart_time(arr)',
            'arrival_time_in_arr': 'arrival_time(arr)',
            'seat_class': 'seat_class',
            'agt_code': 'agt_code',
            'fare_class':'fare_class',
            'adult_fare': 'fare',
            'journey_time': 'journey_time'
        }
        df = df.rename(columns=column_mapping)
        
        # 날짜 처리
        date_columns = ['fetched_date', 'depart_time(dep)']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])

        df['fetched_date'] = df['fetched_date'].dt.date
        df['depart_date'] = df['depart_time(dep)'].dt.date
        
        # 날짜 범위 계산
        max_fetched_date = df['fetched_date'].max()
        
        return df, max_fetched_date
    
    def get_scenarios(self, df, max_fetched_date):
        result = {
            "flights": {},
            "prices": {}
        }
        
        # 1. flight_info 추출 및 식별자 정의
        for air_id, air_group in df.groupby('air_id'):
            first_record = air_group.iloc[0]
            result["flights"][air_id] = {
                'depart_time(utc)': first_record['depart_time(utc)'],
                'depart_time(dep)': first_record['depart_time(dep)'],
                'arrival_time(utc)': first_record['arrival_time(utc)'],
                'arrival_time(arr)': first_record['arrival_time(arr)'],
                'depart_airport': first_record['airport_code(dep)'],
                'arrival_airport': first_record['airport_code(arr)'],
                'is_layover': str(first_record['is_layover']),
                'journey_time': first_record['journey_time']
            }
            
            # 2. 각 항공편별 가격 옵션 처리
            result["prices"][air_id] = {}
            
            # 각 좌석 클래스, 항공사, 대행사, 운임 클래스 별로 그룹화
            for (seat_class, airline, agency, fare_class), option_group in air_group.groupby(
                ['seat_class', 'airline', 'agt_code', 'fare_class']
            ):
                # 옵션 식별자 생성
                option_id = f"{seat_class}.{airline}.{agency}.{fare_class}"
                
                # 가격 이력 생성
                dep_date = option_group['depart_date'].iloc[0]
                min_fetched_date = option_group['fetched_date'].min()
                
                # 날짜 범위 계산
                max_dep_date = max_fetched_date + timedelta(1)
                diff_days = (min(dep_date, max_dep_date) - min_fetched_date).days
                
                price_history = []
                for i in range(diff_days):
                    target_date = min_fetched_date + timedelta(i)
                    day_record = option_group[option_group['fetched_date'] == target_date]
                    
                    if len(day_record) == 0:
                        price_history.append({
                            'collected_date': target_date.strftime('%Y-%m-%d'),
                            'collected_day': get_weekday_kr(target_date),
                            'days_to_departure': (dep_date - target_date).days,
                            'price': None,
                            'is_available': False
                        })
                    else:
                        price_history.append({
                            'collected_date': target_date.strftime('%Y-%m-%d'),
                            'collected_day': get_weekday_kr(target_date),
                            'days_to_departure': (dep_date - target_date).days,
                            'price': int(day_record['fare'].iloc[0]),
                            'is_available': True
                        })
                
                # 결과에 가격 이력 추가
                result["prices"][air_id][option_id] = {
                    "metadata": {
                        "seat_class": seat_class,
                        "airline": airline,
                        "agency": agency,
                        "fare_class": fare_class
                    },
                    "price_history": price_history
                }
        
        return result
    @classmethod
    def formatting_layover_infos(cls, df):
        result = {}
        
        for air_id, group in df.groupby('air_id'):
            segments = []
            
            for _, row in group.iterrows():
                segment = {
                    "segment_id": row["segment_id"],
                    "segment_order": row["layover_order"],
                    "connect_time": row["connect_time"],
                    "airline": row["segment_airline"],
                    "from": row["segment_depart"],
                    "to": row["segment_arrival"],
                    "depart_time_utc": str(row["segment_depart_time_utc"]),
                    "arrival_time_utc": str(row["segment_arrival_time_utc"]),
                    # "depart_time_in_dep": str(row["segment_depart_time_in_dep"]),
                    # "arrival_time_in_dep": str(row["segment_arrival_time_in_dep"]),
                    # "depart_time_in_arr": str(row["segment_depart_time_in_arr"]),
                    # "arrival_time_in_arr": str(row["segment_arrival_time_in_arr"]),
                    "duration": row["segment_duration"]
                }
                segments.append(segment)
            
            result[air_id] = segments
        
        return result
    
    @classmethod
    def save_scenarios(cls, scenarios, output_path) -> None:
        """시나리오를 JSON 파일로 저장"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(scenarios, f, ensure_ascii=False, indent=2, default=str)