City Temperature Analysis


Problem Statement	
As a data engineer you are supposed to prepare the data from temp analysis
Your pipeline should return data in form of following columns
city
avg_temperature
total_temperature
num_measurements
You should return metrics for only those cities when total_temperature is greater than 30
And output should be sorted on city in ascending order

Data

 New York , 10.0  
 New York , 12.0 
 Los Angeles , 20.0  
 Los Angeles , 22.0 
 San Francisco , 15.0  
 San Francisco , 18.0

Metadata- columns

city - String
temperature - Double

Solution:

Python code for to solve above problem is as follows:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count


def city_temp_analyzer(data):
    # Step 3: Define the schema (columns)
    columns = ["city", "temperature"]
    # Step 4: Create DataFrame
    df = spark.createDataFrame(data, schema=columns)
    # Step 5: Group data by 'city' and calculate total_temperature, avg_temperature, num_measurements
    df_grouped = df.groupBy("city").agg(
        _sum("temperature").alias("total_temperature"),
        avg("temperature").alias("avg_temperature"),
        count("temperature").alias("num_measurements")
    )
    # Step 6: Filter cities where total_temperature > 30
    df_filtered = df_grouped.filter(col("total_temperature") > 30)
    # Step 7: Sort the output by 'city' in ascending order
    df_sorted = df_filtered.orderBy("city")
    # Step 8: Show the result
    df_sorted.show()


if __name__ == '__main__':
    # Step 1: Create a SparkSession
    spark = SparkSession.builder.appName("City Temperature Analysis").getOrCreate()

    # Step 2: Sample Data
    data = [
        ("New York", 10.0),
        ("New York", 12.0),
        ("Los Angeles", 20.0),
        ("Los Angeles", 22.0),
        ("San Francisco", 15.0),
        ("San Francisco", 18.0)
]

    city_temp_analyzer(data)

# Step 9: Stop the SparkSession
spark.stop()
