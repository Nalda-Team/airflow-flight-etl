from airflow.providers.redis.sensors.redis_key import RedisKeySensor
from airflow.providers.redis.hooks.redis import RedisHook
from typing import Sequence

class RedisKeyValueSensor(RedisKeySensor):
    """확장된 Redis 센서: 키의 존재 및 값을 확인합니다."""

    template_fields: Sequence[str] = ("key", "expected_value")

    def __init__(self, *, expected_value=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.expected_value = expected_value

    def poke(self, context) -> bool:
        conn = RedisHook(self.redis_conn_id).get_conn()
        
        # 키가 존재하는지 확인
        if not conn.exists(self.key):
            return False
            
        # expected_value가 설정된 경우 값도 확인
        if self.expected_value is not None:
            value = conn.get(self.key)
            # Redis에서 가져온 값은 바이트 형태일 수 있으므로 디코딩
            if isinstance(value, bytes):
                value = value.decode('utf-8')

            if value==self.expected_value:
                print(f'{self.expected_value}가 확인되었습니다!')
            return value == self.expected_value
            
        return True