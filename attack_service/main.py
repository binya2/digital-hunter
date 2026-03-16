from pydantic import ValidationError

from shared.kafka import kafka_service
from shared.logger import log_event
from shared.modools import AttackEvent
from shared.postgres import postgres_service as ps


def process_attack(attack: dict):
    try:
        valid_attack = AttackEvent(**attack)

        check_query = """SELECT damage_status
                         FROM targets
                         WHERE entity_id = %s"""
        result = ps.execute_query(query=check_query, params=(valid_attack.entity_id,), fetch=True)

        if not result or result[0].get('damage_status') == 'destroyed':
            raise ValueError(f"Logical Error: Received attack report for unknown entity_id: {valid_attack.entity_id}")

        query: str = """UPDATE targets
                        SET last_attack_id= %s,
                            last_weapon_type= %s
                        WHERE entity_id = %s"""
        params: tuple = (valid_attack.attack_id,
                         valid_attack.weapon_type,
                         valid_attack.entity_id)
        affected_rows = ps.execute_query(query=query, params=params)

        if affected_rows == 0:
            raise ValueError(f"Logical Error: Received attack report for unknown entity_id: {valid_attack.entity_id}")

    except (ValidationError, ValueError) as e:
        error_payload = {
            "original_message": attack,
            "error_reason": str(e),
            "source": "attack_service"
        }
        kafka_service.producer_message(next_event=error_payload)
        log_event("Error",f"ATTACK - Faulty message routed to intel_signals_dlq. Reason: {e}")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_attack)
