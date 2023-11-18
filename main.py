from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import expr, from_unixtime, unix_timestamp, col

# Initialize a SparkSession
spark = SparkSession.builder.appName("PYSPARK").getOrCreate()

# File path
df_path = 'dataset.txt'

# Read the data with inferred schema
df = spark.read.csv(df_path, header=True, inferSchema=True)

# Data processing function
def ql(data):
    # Transform the Timestamp column (if necessary)
    data = data.withColumn("Timestamp", (col("Timestamp") + 1 / 3))

    # Merge the Date and Time columns into a single DateTime column
    data = data.withColumn("DateTime", expr("concat(Date, ' ', Time)"))

    # Convert DateTime into a standard datetime format
    data = data.withColumn("DateTime",
                           from_unixtime(unix_timestamp(col("DateTime"), "yyyy-MM-dd HH:mm:ss")))

    # Extract the date and time
    data = data.withColumn("Date", from_unixtime(unix_timestamp(col("DateTime")), "yyyy-MM-dd"))
    data = data.withColumn("Time", from_unixtime(unix_timestamp(col("DateTime")), "HH:mm:ss"))

    # Drop the intermediate DateTime column
    data = data.drop("DateTime")

    return data

# Process the data
df = ql(df)

# Display the results
print("Question 1: ")
df.show()
