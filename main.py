from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime, col, expr

# Start a SparkSession
spark = SparkSession.builder.appName("PYSPARK").getOrCreate()

# Set the file path
df_path = 'dataset.txt'

# Read the data with inferred schema to auto judge data types
df = spark.read.csv(df_path, header=True, inferSchema=True)
# To keep the Date format correct
df = df.withColumn("Date", F.date_format("Date", "yyyy-MM-dd"))


# Task 1 function
def task_1(data):
    # Use unix_timestamp to convert a "Date and Time" (with format: "yyyy-MM-dd HH:mm:ss") String to unix_timestamp
    # unix_timestamp is in long format (indicate how many seconds after 1/1/1970) (Prepare for time zone convert)
    # Use expr to perform SQL command, and use concate to merge Date and Time
    timestamp = unix_timestamp(expr("concat(Date, ' ', Time)"), "yyyy-MM-dd HH:mm:ss").cast("long")

    # Adjust the timestamp for the China time zone
    # 28800 is in seconds format and equal to 8 hours
    # Use cast("timestamp") to convert seconds to standard timestamp
    china_standard_timestamp = (timestamp + 28800).cast("timestamp")

    # Convert timestamp into String format
    # Use F.date_format to convert a timestamp to the format "yyyy-MM-dd" or "HH:mm:ss"
    # Use data.withColumn to Replace the previous Time and Date columns with the new China time.
    data = data.withColumn("Date", F.date_format(china_standard_timestamp, "yyyy-MM-dd"))
    data = data.withColumn("Time", F.date_format(china_standard_timestamp, "HH:mm:ss"))
    # Update timestamp by adding 8/24
    data = data.withColumn("Timestamp", df["Timestamp"] + 8/24)

    return data


# Process the data
df_task_1 = task_1(df)

# Display the results
print("Task 1: ")
df_task_1.show()


def task_2(df):
    # Calculate the count of records for each user per day
    count_per_day = df.groupBy("UserID", "Date").count()
    #count_per_day.show()
    # Filter out the records where a user has at least five data points in a day
    at_least_five = count_per_day.filter(count_per_day['count'] >= 5)
    # at_least_five.show()
    # Calculate the number of days each user has at least five data points
    days_per_user = at_least_five.groupBy("UserID").count()
    # days_per_user.show()
    # Sort users by the number of qualifying days in descending order, and then by UserID in ascending order
    top_users = days_per_user.orderBy(F.desc("count"), F.asc("UserID"))

    # Select the top 5 users
    top_five_users = top_users.limit(5)

    # Return the result
    return top_five_users


# Display the results
print("Task 2: ")
top_five_users = task_2(df)
top_five_users.show()


def task_3(df):
    # Add a column for the week number
    df = df.withColumn("Week", F.weekofyear(F.to_date("Date")))

    # Calculate the count of records for each user per week
    count_per_week = df.groupBy("UserID", "Week").count()

    # Filter out the records where a user has more than 100 data points in a week
    more_than_100 = count_per_week.filter(count_per_week['count'] > 100)

    # Calculate the number of weeks each user has more than 100 data points
    weeks_per_user = more_than_100.groupBy("UserID").count()

    # Return the result
    return weeks_per_user


# Display the results
print("Task 3: ")
weeks_per_user = task_3(df)
weeks_per_user.show()
