import faust
from taxi_rides import TaxiRide

app = faust.App(
    "datatalksclub.stream.v2.final",
    broker="kafka://localhost:9092",
    web_port=6067,
)

topic = app.topic(
    "datatalkclub.yellow_taxi_ride.json",
    value_type=TaxiRide,
)

vendor_rides = app.Table("vendor_rides", default=int)

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(
        lambda e: e.vendorId,
        name="by_vendor"
    ):
        vendor_rides[event.vendorId] += 1
        print(
            "vendorId:",
            event.vendorId,
            "count:",
            vendor_rides[event.vendorId],
        )

if __name__ == "__main__":
    app.main()
