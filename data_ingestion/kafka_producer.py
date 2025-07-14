from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_messages():
    for i in range(10):
        message = {"ride_id": i, "pickup_location": "Manhattan", "amount": round(10 + i * 1.5, 2)}
        producer.send('nyc_taxi_rides', message)
        print(f"Sent: {message}")
        time.sleep(1)

if __name__ == "__main__":
    produce_messages()
