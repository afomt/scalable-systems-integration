import os, asyncio
from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "customer_data")

async def consume_forever():
    while True:
        consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id="analytics-consumer",
            auto_offset_reset="earliest"
        )

        print("will try connecting..", flush=True)

        try:
            await consumer.start()
            print("Kafka consumer connected  ", flush=True)

            async for msg in consumer:
                print(msg.value, flush=True)

        except Exception as e:
            print(f"Kafka error: {e}. Retrying in 5s...", flush=True)
            await asyncio.sleep(5)

        finally:
            try:
                await consumer.stop()
            except:
                pass

asyncio.run(consume_forever())
