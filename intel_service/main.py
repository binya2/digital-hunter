from shared.config import settings
from shared.haversine import haversine_km
from shared.kafka import kafka_service
from shared.modools import IntelEvent
from shared.postgres import postgres_service as ps


def process_intel(signal: dict):
    valid_signal = IntelEvent(**signal)

    query = "SELECT last_lat, last_lon FROM %s WHERE entity_id = %s"
    params = (settings.POSTGRES_DATABASE, valid_signal.entity_id)
    db_target = ps.execute_query(query=query, params=params, fetch=True)[0]
    if db_target:
        dist = haversine_km(db_target[0], db_target[1], valid_signal.reported_lat, valid_signal.reported_lon)
        print(f"[INTEL] Target {valid_signal.entity_id} moved {dist:.2f} km (DB Hit)")

        query = "UPDATE targets SET last_lat = %s, last_lon = %s WHERE entity_id = %s"
        params = (valid_signal.reported_lat, valid_signal.reported_lon, valid_signal.entity_id)
        ps.execute_query(query=query, params=params)

    else:
        valid_signal.priority_level = 99
        print(f"[INTEL] New Target {valid_signal.entity_id}. Priority set to 99.")
        query = "INSERT INTO targets (entity_id, last_lat, last_lon, priority_level) VALUES (%s, %s, %s, %s)"
        params = (valid_signal.entity_id, valid_signal.reported_lat, valid_signal.reported_lon,
                  valid_signal.priority_level)
        ps.execute_query(query=query, params=params)


if __name__ == "__main__":
    kafka_service.start_generic_consumer(process_intel)
