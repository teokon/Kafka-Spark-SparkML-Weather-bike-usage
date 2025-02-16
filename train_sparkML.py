from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TrainModelWithoutCityName") \
    .getOrCreate()

# Read the CSV file from DBFS using the file URI scheme.
csv_path = "../combined_file.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)
df.printSchema()
df.show(5, truncate=False)

# The CSV is assumed to have the following columns:
# "city_name", "temperature", "wind_speed", "precipitation", "cloudiness", "average_docking_station_utilisation"
# We will ignore "city_name" in our model.

# -------------------------------------
# Define the Pipeline Stages
# -------------------------------------

# Stage 1: Assemble the features (excluding the "city_name" column)
assembler = VectorAssembler(
    inputCols=["temperature", "wind_speed", "cloudiness"],
    outputCol="features"
)

# Stage 2: Define a Linear Regression model (or another regressor)
lr = LinearRegression(featuresCol="features", labelCol="avg_utilization")

# Create the Pipeline with the stages (only assembler and lr)
pipeline = Pipeline(stages=[assembler, lr])

# -------------------------------------
# Split the Data into Training and Test Sets
# -------------------------------------
(training_data, test_data) = df.randomSplit([0.7, 0.3], seed=42)
print("Training Data Count: ", training_data.count())
print("Test Data Count: ", test_data.count())

# -------------------------------------
# Train the Model Using the Pipeline
# -------------------------------------
pipeline_model = pipeline.fit(training_data)

# Make predictions on the test set
predictions = pipeline_model.transform(test_data)
predictions.select("features", "avg_utilization", "prediction").show(5, truncate=False)

# -------------------------------------
# Evaluate the Model
# -------------------------------------
evaluator = RegressionEvaluator(
    labelCol="avg_utilization",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Optionally, save the trained pipeline model for later use
# pipeline_model.write().overwrite().save("file:///C:/Users/konth/Downloads/pipeline_model")
pipeline_model.write().overwrite().save("/home/observer/PycharmProjects/PySparkProject/Final/sparkML")
