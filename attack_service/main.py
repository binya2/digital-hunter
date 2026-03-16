from pydantic import ValidationError

from shared.kafka import kafka_service
from shared.modools import AttackEvent
from shared.postgres import postgres_service as ps


def process_attack(attack: dict):
    try:
        valid_attack = AttackEvent(**attack)
        query: str = "UPDATE targets SET last_attack_id= %s, last_weapon_type= %s WHERE entity_id=%s"
        params: tuple = (valid_attack.attack_id, valid_attack.weapon_type, valid_attack.entity_id)
        ps.execute_query(query=query, params=params)
        print(f"[ATTACK] Recorded attack {attack.attack_id} on {attack.entity_id}")
    except ValidationError:
        print("[ERROR] Invalid attack report format.")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_attack)
