import os
import time
import json
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import requests

# Set up logging
logging.basicConfig(
    filename="citibike_kafka.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Script started")

# Set up the Kafka producer with the correct broker address and serializers
producer = KafkaProducer(
    bootstrap_servers=['150.140.142.67:9094'],  # Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8')
)

# Set up Kafka Admin client to create topics
admin_client = KafkaAdminClient(
    bootstrap_servers=['150.140.142.67:9094']
)

# URLs for Citi Bike New York
STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

# Weather API settings (replace 'your_api_key_here' with your actual API key)
OPENWEATHER_API_KEY = "fd489021c7965ed1e2c75f1ab2994366"
WEATHER_URL = f"http://api.openweathermap.org/data/2.5/weather?q=New%20York,US&appid={OPENWEATHER_API_KEY}&units=metric"

# Kafka settings
KAFKA_BROKER = "150.140.142.67:9094"  # Adjust as necessary

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def create_kafka_topics(topics, num_partitions=1, replication_factor=1):
    """
    Creates the Kafka topics if they do not already exist.
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        # Retrieve the list of existing topics
        existing_topics = admin_client.list_topics()
        new_topics = []
        for topic in topics:
            if topic not in existing_topics:
                new_topics.append(NewTopic(name=topic,
                                           num_partitions=num_partitions,
                                           replication_factor=replication_factor))
        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
            logging.info(f"Created topics: {[t.name for t in new_topics]}")
        else:
            logging.info("The required topics already exist.")
    except Exception as e:
        logging.error(f"Error creating Kafka topics: {e}")


def fetch_and_send_station_status(url, topic, global_timestamp):
    """
    Fetches data from the station_status_url and sends for each station:
      - station_id
      - timestamp (global timestamp)
      - num_bikes_available
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Station status: Successfully fetched data from {url}.")

        if "data" in data and "stations" in data["data"]:
            stations = data["data"]["stations"]

            for station in stations:
                message = {
                    "station_id": station.get("station_id"),
                    "timestamp": global_timestamp,
                    "num_bikes_available": station.get("num_bikes_available")
                }
                # Create key: f"{station_id}_{timestamp}"
                key = f"{station.get('station_id')}_{global_timestamp}"
                producer.send(topic, key=key.encode("utf-8"), value=message)
                logging.info(f"Station status: Sent message for station_id {station.get('station_id')}")

            producer.flush()
            logging.info(f"Station status: All messages sent to Kafka topic {topic}.")
        else:
            logging.error("Station status: The data structure is not as expected.")

    except requests.RequestException as e:
        logging.error(f"Station status: Failed to fetch data from {url}: {e}")
    except Exception as e:
        logging.error(f"Station status: Error sending data to Kafka: {e}")


def fetch_and_send_station_info(url, topic, global_timestamp):
    """
    Fetches data from the station_info_url and sends for each station:
      - station_id
      - metadata.timestamp (global timestamp)
      - capacity
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Station info: Successfully fetched data from {url}.")

        if "data" in data and "stations" in data["data"]:
            stations = data["data"]["stations"]

            for station in stations:
                message = {
                    "station_id": station.get("station_id"),
                    "metadata": {
                        "timestamp": global_timestamp
                    },
                    "capacity": station.get("capacity")
                }
                key = f"{station.get('station_id')}_{global_timestamp}"
                producer.send(topic, key=key.encode("utf-8"), value=message)
                logging.info(f"Station info: Sent message for station_id {station.get('station_id')}")

            producer.flush()
            logging.info(f"Station info: All messages sent to Kafka topic {topic}.")
        else:
            logging.error("Station info: The data structure is not as expected.")

    except requests.RequestException as e:
        logging.error(f"Station info: Failed to fetch data from {url}: {e}")
    except Exception as e:
        logging.error(f"Station info: Error sending data to Kafka: {e}")


def fetch_and_send_weather_data(url, topic, global_timestamp):
    """
    Fetches data from the Weather API and sends:
      - temperature (from main.temp)
      - windspeed (from wind.speed)
      - clouds (from clouds.all)
      - metadata.timestamp (global timestamp)
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Weather: Successfully fetched data from {url}.")

        # Extract the required fields
        temperature = data.get("main", {}).get("temp")
        windspeed = data.get("wind", {}).get("speed")
        clouds = data.get("clouds", {}).get("all")

        message = {
            "temperature": temperature,
            "windspeed": windspeed,
            "clouds": clouds,
            "metadata": {
                "timestamp": global_timestamp
            }
        }

        if "id" in data:
            key = f"{data['id']}_{global_timestamp}"
        else:
            key = f"weather_{global_timestamp}"

        producer.send(topic, key=key.encode("utf-8"), value=message)
        producer.flush()
        logging.info(f"Weather: Sent message to Kafka topic {topic} with key {key}.")

    except requests.RequestException as e:
        logging.error(f"Weather: Failed to fetch data from {url}: {e}")
    except Exception as e:
        logging.error(f"Weather: Error sending data to Kafka: {e}")


def fetch_all_data():
    """
    Generates a global timestamp and calls the functions for:
      - station_status
      - station_info
      - weather
    """
    global_timestamp = datetime.utcnow().isoformat() + "Z"

    # Define Kafka topics
    station_status_topic = "station_status_1084523_2"
    station_info_topic = "station_info_1084523_2"
    weather_topic = "weather_data_1084523_2"

    fetch_and_send_station_status(STATION_STATUS_URL, station_status_topic, global_timestamp)
    fetch_and_send_station_info(STATION_INFO_URL, station_info_topic, global_timestamp)
    fetch_and_send_weather_data(WEATHER_URL, weather_topic, global_timestamp)


def main():
    # Create the required Kafka topics
    topics_to_create = ["station_status_1084523_2", "station_info_1084523_2", "weather_data_1084523_2"]
    create_kafka_topics(topics_to_create)

    while True:
        fetch_all_data()
        time.sleep(300)  # Wait for 5 minutes


if __name__ == "__main__":
    main()