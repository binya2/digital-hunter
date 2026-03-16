from pydantic import ValidationError

from shared.haversine import haversine_km
from shared.kafka import kafka_service
from shared.logger import log_event
from shared.modools import IntelEvent
from shared.postgres import postgres_service as ps


def average_speed(dist_km, db_target, valid_signal):
    dist_meters = dist_km * 1000
    last_time = db_target['date_time_updating']
    current_time = valid_signal.timestamp
    time_diff_seconds = (current_time - last_time).total_seconds()

    if time_diff_seconds > 0:
        return dist_meters / time_diff_seconds
    return 0.0


def process_intel(signal: dict):
    try:
        valid_signal = IntelEvent(**signal)

        query: str = """SELECT *
                        FROM targets
                        WHERE entity_id = %s"""
        params: tuple = (valid_signal.entity_id,)
        results = ps.execute_query(query=query, params=params, fetch=True)

        if results:
            db_target = results[0]

            if db_target.get('damage_status') == 'destroyed':
                raise ValueError(f"Received intelligence on an already destroyed target: {valid_signal.entity_id}")
            dist_km = haversine_km(db_target['last_lat'], db_target["last_lon"], valid_signal.reported_lat,
                                   valid_signal.reported_lon)
            speed_mps = average_speed(dist_km, db_target, valid_signal)

            query = """UPDATE targets
                       SET last_lat           = %s,
                           last_lon           = %s,
                           date_time_updating = %s,
                           priority_level     = %s,
                           distance           = %s,
                           avg_speed          = %s
                       WHERE entity_id = %s"""
            params = (valid_signal.reported_lat,
                      valid_signal.reported_lon,
                      valid_signal.timestamp,
                      valid_signal.priority_level,
                      dist_km,
                      speed_mps,
                      valid_signal.entity_id)
            ps.execute_query(query=query, params=params)

        else:
            valid_signal.priority_level = 99
            query = """INSERT INTO targets (entity_id,
                                            last_lat,
                                            last_lon,
                                            priority_level,
                                            date_time_creation,
                                            date_time_updating,
                                            distance,
                                            avg_speed)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            params = (valid_signal.entity_id,
                      valid_signal.reported_lat,
                      valid_signal.reported_lon,
                      valid_signal.priority_level,
                      valid_signal.timestamp,
                      valid_signal.timestamp,
                      0.0,
                      0.0)
            ps.execute_query(query=query, params=params)
    except (ValidationError, ValueError) as e:
        error_payload = {
            "original_message": signal,
            "error_reason": str(e),
            "source": "intel_service"
        }
        kafka_service.producer_message(next_event=error_payload)
        log_event("Error",f"INTEL - Faulty message routed to intel_signals_dlq. Reason: {e}")


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_intel)
