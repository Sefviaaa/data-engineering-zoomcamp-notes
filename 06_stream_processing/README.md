# 06_stream_processing - Stream Processing with Kafka

This module covers stream processing concepts and hands-on implementation using Apache Kafka, as part of the Data Engineering Zoomcamp.

## Contents

| File/Folder | Description |
|-------------|-------------|
| `docker-compose.yml` | Kafka and Zookeeper services |
| `producer.py` | Sample Kafka producer |
| `kafka-python-demo/` | Complete Kafka Python examples |

## Kafka Python Demo

The `kafka-python-demo/` folder contains:

| File | Description |
|------|-------------|
| `producer.py` | Produce taxi ride events to Kafka |
| `consumer.py` | Consume events from Kafka |
| `stream.py` | Faust streaming application |
| `windowing.py` | Windowed aggregations |
| `*.avsc` | Avro schemas for taxi rides |

## Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Run Producer

```bash
cd kafka-python-demo
pip install -r requirements.txt
python producer.py
```

### 3. Run Consumer

```bash
python consumer.py
```

## Topics Covered

- **Apache Kafka**: Topics, producers, consumers, partitions
- **Kafka with Python**: confluent-kafka, Faust
- **Stream Processing**: Windowing, aggregations
- **Avro**: Schema registry, serialization
