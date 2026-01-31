import faust
from taxi_rides import TaxiRide

app = faust.App(
    "datatalksclub.stream.v3",
    broker="kafka://localhost:9092",
    consumer_auto_offset_reset="earliest",
)

source = app.topic(
    "datatalkclub.yellow_taxi_ride.json",
    value_type=TaxiRide,
)

high_amount_rides = app.topic("datatalks.yellow_taxi_rides.high_amount")
low_amount_rides = app.topic("datatalks.yellow_taxi_rides.low_amount")


@app.agent(source)
async def process(stream):
    async for event in stream:
        if event.total_amount >= 40.0:
            await high_amount_rides.send(value=event)
            print("HIGH:", event.total_amount)
        else:
            await low_amount_rides.send(value=event)
            print("LOW:", event.total_amount)


if __name__ == "__main__":
    app.main()
