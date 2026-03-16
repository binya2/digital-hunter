from pydantic import ValidationError

from shared.kafka import kafka_service
from shared.logger import log_event
from shared.modools import DamageEvent
from shared.postgres import postgres_service as ps


def process_damage(damage: dict):
    try:
        valid_damage = DamageEvent(**damage)

        query: str = """UPDATE targets
                        SET last_attack_id= %s,
                            damage_status = %s,
                            date_time_updating = %s
                        WHERE entity_id = %s"""
        params: tuple = (valid_damage.attack_id,
                         valid_damage.result,
                         valid_damage.timestamp,
                         valid_damage.entity_id)
        ps.execute_query(query=query, params=params)
    except ValidationError:
        raise Exception("Invalid damage message")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_damage)
