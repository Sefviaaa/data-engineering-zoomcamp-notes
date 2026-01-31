import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda x: dumps(x).encode("utf-8"),
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

with open("data/rides.csv", newline="") as file:
    csvreader = csv.DictReader(file)  # ✅ FIX

    for row in csvreader:
        key = {"vendorId": int(row["VendorID"])}

        value = {
            "vendorId": int(row["VendorID"]),
            "passenger_count": int(row["passenger_count"]),
            "trip_distance": float(row["trip_distance"]),
            "payment_type": int(row["payment_type"]),
            "total_amount": float(row["total_amount"]),
        }

        producer.send(
            "datatalkclub.yellow_taxi_ride.json",
            key=key,
            value=value,
        )

        print("producing", value)
        sleep(1)
