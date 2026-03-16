from pydantic import ValidationError

from shared.kafka import kafka_service
from shared.modools import DamageEvent
from shared.postgres import postgres_service as ps


def process_damage(damage: dict):
    try:
        valid_damage = DamageEvent(**damage)

        query: str = "UPDATE targets SET last_attack_id= %s, last_weapon_type= %s WHERE entity_id= %s"
        params: tuple = (valid_damage.result, valid_damage.entity_id)
        ps.execute_query(query=query, params=params)

        print(f"[DAMAGE] Target {valid_damage.entity_id} status updated to: {valid_damage.result}")
    except ValidationError:
        print("[ERROR] Invalid damage report format.")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_damage)
