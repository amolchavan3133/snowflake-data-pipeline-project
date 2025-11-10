import time
import json
import requests  # To fetch data from the API
from kafka import KafkaProducer

# --- Connect to Kafka ---
# This connects to the Kafka broker you have running in Docker
# We use a JSON serializer to automatically convert our data to JSON
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Connected to Kafka! Press Ctrl+C to stop.")

# --- Get Live Data (No API Key needed!) ---
# We will get weather data for Pune from the free Open-Meteo API
# This is a simple API that doesn't require an account
api_url = "https://api.open-meteo.com/v1/forecast?latitude=18.52&longitude=73.86&current_weather=true"

# --- Loop and Send Data ---
# We will fetch and send new data every 10 seconds
try:
    while True:
        # 1. Fetch data from the API
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            # We only care about the 'current_weather' part
            weather_data = data['current_weather']

            # 2. Send data to Kafka
            # We send it to a "topic" named 'weather_data'
            producer.send('weather_data', weather_data)

            print("Sent data to Kafka:")
            print(weather_data)

        else:
            print(f"Failed to get data from API. Status code: {response.status_code}")

        # Wait for 10 seconds before fetching again
        time.sleep(10)

except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.close()