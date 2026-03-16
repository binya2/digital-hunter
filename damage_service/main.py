from pydantic import ValidationError

from shared.kafka import kafka_service
from shared.logger import log_event
from shared.modools import DamageEvent
from shared.postgres import postgres_service as ps


def process_damage(damage: dict):
    try:
        valid_damage = DamageEvent(**damage)
        check_query = """SELECT entity_id, damage_status
                         FROM targets
                         WHERE last_attack_id = %s"""
        attack_exists = ps.execute_query(query=check_query, params=(str(valid_damage.attack_id),), fetch=True)

        if not attack_exists or attack_exists[0].get('damage_status') == 'destroyed':
            raise ValueError(
                f"Logical Error: Damage reported for an unknown or fake attack_id: {valid_damage.attack_id}")

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
    except (ValidationError, ValueError) as e:
        error_payload = {
            "original_message": damage,
            "error_reason": str(e),
            "source": "damage_service"
        }
        kafka_service.producer_message(next_event=error_payload)
        log_event("Error","DAMAGE - Faulty message routed to intel_signals_dlq. Reason: {e}")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_damage)
