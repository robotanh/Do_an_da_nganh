import json
from confluent_kafka import Producer

# Configuration for producer
conf = {
    'bootstrap.servers': 'localhost:19092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'superuser',
    'sasl.password': 'secretpassword'
}

# Create Producer instance
producer = Producer(conf)

# Optional callback for delivery reports
def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to produce JSON messages
def produce_json_messages(topic, num_messages):
    for i in range(num_messages):

        message = {
            "user_id": i,
            "name": "User" + str(i),
            "email": f"user{i}@example.com",
            "login_time": "2024-09-23T10:00:00Z",
            "product_url": "https://example.com/product",
            "price": "10.99 USD",
            "timestamp": "2024-09-23T10:05:00Z"
        }
        

        json_message = json.dumps(message)

        # Produce message to the topic
        producer.produce(topic, json_message.encode('utf-8'), callback=delivery_callback)
        print(f"Produced message {i} to topic {topic}")
    
    # Wait for all messages to be sent
    producer.flush()


produce_json_messages('test', 10)
