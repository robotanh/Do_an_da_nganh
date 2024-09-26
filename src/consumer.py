from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',  # Brokers
    'group.id': 'my_consumer_group',  # Consumer group id
    'auto.offset.reset': 'earliest',  # Start from the earliest message
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': 'superuser',
    'sasl.password': 'secretpassword'
}

# Create Consumer instance
consumer = Consumer(consumer_conf)

# Subscribe to topic 'test'
consumer.subscribe(['test'])

# Function to consume messages from 'test' topic
def consume_messages():
    try:
        while True:
            # Poll message from Kafka
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue  # No message received
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition {msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Successfully received a message
                try:
                    # Attempt to parse message as JSON
                    message_value = msg.value().decode('utf-8')
                    json_message = json.loads(message_value)
                    print(f"Consumed message: {json_message}")
                except json.JSONDecodeError:
                    # Handle non-JSON message
                    print(f"Received non-JSON message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass  # Allow clean exit on Ctrl+C
    finally:
        # Close the consumer on exit
        consumer.close()

# Run the consumer
if __name__ == "__main__":
    consume_messages()
