from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime, col, expr
from pyspark.sql.types import FloatType
from math import radians, sin, cos, sqrt, atan2

# Start a SparkSession
spark = SparkSession.builder.appName("PYSPARK").getOrCreate()

# Set the file path
df_path = 'dataset.txt'

# Read the data with inferred schema to auto judge data types
df = spark.read.csv(df_path, header=True, inferSchema=True)
# To keep the Date format correct (Without "Time" IN "Date" to avoid incorrect calculate)
# Because using auto inferSchema, the "Date" will be "yy-mm-dd hh-mm-ss", which leads to mess
df = df.withColumn("Date", F.date_format("Date", "yyyy-MM-dd"))


# Task 1 function
def task_1(dataframe):
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
    dataframe = dataframe.withColumn("Date", F.date_format(china_standard_timestamp, "yyyy-MM-dd"))
    dataframe = dataframe.withColumn("Time", F.date_format(china_standard_timestamp, "HH:mm:ss"))
    # Update timestamp by adding 8/24
    dataframe = dataframe.withColumn("Timestamp", dataframe["Timestamp"] + 8 / 24)

    return dataframe


# Display the results
df_task_1 = task_1(df)
# print("Task 1: ")
# df_task_1.show()


# Task 2 function
def task_2(dataframe):
    # Calculate the count of data points recorded for each user a day
    count_each_usr_a_day = dataframe.groupBy("UserID", "Date").count()
    # count_each_usr_a_day.show()

    # Filter out all items in groups that a user has submitted 5 or more data points in a day
    at_least_five_records = count_each_usr_a_day.filter(count_each_usr_a_day['count'] >= 5)
    # at_least_five_records.groupBy("UserID").count().show()
    # Get the user ID and their days count that have at least five data points, and use agg combines .alias() to change the colum name
    usr_ID_and_count = at_least_five_records.groupBy("UserID").agg(F.count("Date").alias("DaysWithAtLeastFiveRecords"))
    # usr_ID_and_count.show()
    # Sort by the days count in descending order, and  by UserID in ascending order
    top_users = usr_ID_and_count.orderBy(F.desc("DaysWithAtLeastFiveRecords"), F.asc("UserID"))

    # get the top 5 users
    top_five = top_users.limit(5)

    return top_five


# Display the results
# print("Task 2: ")
# top_five_users = task_2(task_1(df))
# top_five_users.show()


# Task 3 function
def task_3(dataframe):
    # Using weekofyear() will cause an incorrect answer when handle the crossing year situation
    # Timestamp - 2 is equal to change the Timestamp to start from 1900-1-1, which is Monday
    # Then it can be simply devided by 7 to calculate which week is today from 1899-12-30,with no worry about crossing different years
    dataframe = dataframe.withColumn("WhichWeek", ((F.col("Timestamp") - 2) / 7).cast('integer'))
    # dataframe.show()
    # Calculate the count of records for every user per week
    count_per_week = dataframe.groupBy("UserID", "WhichWeek").count()

    # Filter out the records that a user has submitted more than 100 data points in one week
    more_than_100_records = count_per_week.filter(count_per_week['count'] > 100)

    # Calculate the number of weeks each user has submitted more than 100 data points
    weeks_a_user = more_than_100_records.groupBy("UserID").agg(
        F.countDistinct("WhichWeek").alias("WeeksMoreThan100Record"))

    # Order by UserID
    return weeks_a_user.orderBy(F.asc("UserID"))


# Display the results
# print("Task 3: ")
# weeks_per_user = task_3(df_task_1)
# weeks_per_user.show()

# Task 4 function

def task_4(dataframe):
    # Get the minimum latitude for each user on each day
    min_latitudes_per_user_per_day = dataframe.groupBy("UserID", "Date").agg(F.min("Latitude").alias("MinLatitude"))
    # min_latitudes_per_user_per_day.show()

    # Get the overall minimum latitudes of each user (which is the Most Southern Latitude),by groupby userID again and get the minimum latitude
    min_latitudes_per_user = min_latitudes_per_user_per_day.groupBy("UserID").agg(
        F.min("MinLatitude").alias("MostSouthernLatitude"))
    # min_latitudes_per_user.show()

    # Rename the column in "min_latitudes_per_user_per_day",ready for joining
    # In this situation "MostSouthernLatitude" is not the real overall MostSouthernLatitude, it's just for joining and matching
    new_min_latitudes_per_day = min_latitudes_per_user_per_day.withColumnRenamed("MinLatitude", "MostSouthernLatitude")
    # new_min_latitudes_per_day.show()

    # Join the dataframes with "UserID" (primary),
    # and use the "overall" MostSouthernLatitude in "min_latitudes_per_user" to join the "Fake" MostSouthernLatitude Colunm in new_min_latitudes_per_day
    # Let the "overall" MostSouthernLatitude to match the "MinLatitude"(Which is the "Fake" MostSouthernLatitude Colunm now) in min_latitudes_per_user_per_day
    # Then will get a form with all information needed
    southernmost_dates = new_min_latitudes_per_day.join(min_latitudes_per_user, ["UserID", "MostSouthernLatitude"])
    # southernmost_dates.show()

    # Get the records with the earliest date
    southernmost_dates = southernmost_dates.groupBy("UserID", "MostSouthernLatitude").agg(
        F.min("Date").alias("FirstDateOfMostSouthern"))

    # Get top 5 users
    top_5_most_southern_users = southernmost_dates.orderBy("MostSouthernLatitude", "UserID").limit(5)

    return top_5_most_southern_users


# Display the results
# print("Task 4: ")
# top_five_users_week = task_4(df_task_1)
# top_five_users_week.show()


# Task 5 function
def task_5(dataframe):
    # The span is the difference between the maximum and minimum altitude
    span = dataframe.groupBy(["UserID", "Date"]).agg((F.max("Altitude") - F.min("Altitude")).alias("AltitudeSpan"))

    # Convert the  feet to meters 1 foot = 0.3048 m, and get the largest Altitude span in a day
    max_span = span.groupBy(["UserID"]).agg((F.max("AltitudeSpan") * 0.3048).alias("MAXSpan"))

    # Order the users in descending order (Using MAX_span) and get top 5 users
    top_five_span_users = max_span.orderBy(F.desc("MAXSpan")).limit(5)

    return top_five_span_users

# Display the results
# print("Task 5: ")
# top_five_span = task_5(df_task_1)
# top_five_span.show()

# Task 6 function
def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on the earth."""
    # 检查经纬度值是否为 None
    if None in (lon1, lat1, lon2, lat2):
        return 0.0  # 如果有任何值为 None，返回距离 0

    # 将经纬度转换为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # 计算经纬度差
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # 哈弗辛公式
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    # 地球半径（千米）
    r = 6371

    return c * r





def task_6(dataframe):
    # Register the UDF
    haversine_udf = F.udf(haversine, FloatType())

    # Define the window specification for lag function
    windowSpec = Window.partitionBy("UserID", "Date").orderBy("Timestamp")

    # Calculate the distance between consecutive points
    dataframe = dataframe.withColumn("PrevLatitude", F.lag("Latitude").over(windowSpec))
    dataframe = dataframe.withColumn("PrevLongitude", F.lag("Longitude").over(windowSpec))
    dataframe = dataframe.withColumn("Distance", haversine_udf("Longitude", "Latitude", "PrevLongitude", "PrevLatitude"))

    # Calculate the total distance traveled by each user each day
    daily_distance = dataframe.groupBy("UserID", "Date").agg(F.sum("Distance").alias("TotalDistance"))

    # Define the window specification for finding the maximum distance day
    windowSpecUser = Window.partitionBy("UserID").orderBy(F.desc("TotalDistance"), F.asc("Date"))

    # Find the day with the maximum distance traveled for each user
    daily_distance = daily_distance.withColumn("Rank", F.row_number().over(windowSpecUser))
    max_distance_day = daily_distance.filter(daily_distance.Rank == 1).select("UserID", "Date").orderBy("UserID")

    # Find the total distance traveled by all users on all days
    total_distance_all = daily_distance.agg(F.sum("TotalDistance").alias("TotalDistanceAll"))

    return max_distance_day, total_distance_all

#Display the results
print("Task 6: ")
max_distance_day, total_distance_all = task_6(df_task_1)

print("每个用户行走最远的一天:")
max_distance_day.show()

# 展示所有用户所有天的总行走距离
print("所有用户所有天的总行走距离:")
total_distance_all.show()