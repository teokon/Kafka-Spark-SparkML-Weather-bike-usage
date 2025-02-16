import requests
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel


spark = SparkSession.builder \
    .appName("KafkaStreamJoin") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

# -------------------------------------------------------------------
# 1. Pull Weather Forecast from OpenWeatherMap API (5-day/3-hour forecast)
# -------------------------------------------------------------------
# Replace these with your actual API key.
OPENWEATHER_API_KEY = "fd489021c7965ed1e2c75f1ab2994366"  # e.g., "fd489021c7965ed1e2c75f1ab2994366"
# Use New York City coordinates: latitude 40.73, longitude -73.93.
WEATHER_URL = (
    f"http://api.openweathermap.org/data/2.5/forecast?"
    f"lat=40.73&lon=-73.93&appid={OPENWEATHER_API_KEY}&units=metric"
)

response = requests.get(WEATHER_URL)
if response.status_code == 200:
    data = response.json()
    forecast_list = data.get("list", [])
    if len(forecast_list) > 0:
        # Use the first forecast available.
        # Note: Forecasts are given in 3-hour intervals.
        forecast = forecast_list[0]

        # Extract forecasted features from the forecast object.
        # The structure for each forecast:
        # - "main": contains "temp", etc.
        # - "wind": contains "speed"
        # - "rain": may contain "3h"
        # - "clouds": contains "all"
        temperature = forecast.get("main", {}).get("temp")
        wind_speed = forecast.get("wind", {}).get("speed")
        # precipitation = forecast.get("rain", {}).get("3h", 0.0) if forecast.get("rain") else 0.0
        cloudiness = forecast.get("clouds", {}).get("all", 0)
        # Convert forecast timestamp from Unix time to human-readable format.
        forecast_dt = datetime.datetime.fromtimestamp(forecast.get("dt"))

        print("Forecast for the next available period (approx. 3 hours ahead):")
        print(f"Time: {forecast_dt}")
        print(f"Temperature: {temperature} °C")
        print(f"Wind Speed: {wind_speed} m/s")
        # print(f"Precipitation (3h): {precipitation} mm")
        print(f"Cloudiness: {cloudiness} %")
    else:
        print("Not enough forecast data available.")
        spark.stop()
        exit(1)
else:
    print("Failed to fetch data from OpenWeatherMap API")
    print("Status Code:", response.status_code)
    print("Response:", response.text)
    spark.stop()
    exit(1)

# -------------------------------------------------------------------
# 2. Create a Spark DataFrame with the Forecast Data
# -------------------------------------------------------------------
schema = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    # StructField("precipitation", DoubleType(), True),
    StructField("cloudiness", IntegerType(), True)
])

# Create a single-row DataFrame with the forecasted weather values.
forecast_row = [(temperature, wind_speed, cloudiness)]
forecast_df = spark.createDataFrame(forecast_row, schema)
forecast_df.show(truncate=False)

# -------------------------------------------------------------------
# 3. Load the Trained ML Model from DBFS
# -------------------------------------------------------------------
model_path = "/home/observer/PycharmProjects/PySparkProject/Final/sparkML"
loaded_model = PipelineModel.load(model_path)

# -------------------------------------------------------------------
# 4. Generate Prediction Using the Loaded Model
# -------------------------------------------------------------------

predictions = loaded_model.transform(forecast_df)

# Display the resulting features vector and the prediction.
predictions.select("features", "prediction").show(truncate=False)

# Optionally, extract the predicted value.
predicted_utilization = predictions.select("prediction").collect()[0][0]
print("Predicted bike utilization for the forecast period:", predicted_utilization)