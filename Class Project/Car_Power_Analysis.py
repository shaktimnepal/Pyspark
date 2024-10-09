Problem Statement:

As a data engineer you are given the task to transform the given data
In addition to given metadata, transformed data should have a column named as AvgWeight 
with constant value as 200 and also kilowatt_power which needs to be 1000 times horsepower.
Column name carr is a mis-spelled column name and you are supposed to correct this to car

Data

Ford Torino, 140, 3449, US
Chevrolet Monte Carlo, 150, 3761, US
BMW 2002, 113, 2234, Europe


Metadata- columns

carr - String
horsepower - Integer
weight - Integer
origin - String

Solution:

Python code to solve the given problem:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def car_power_generator(data):
    # Step 3: Define columns metadata
    columns = ["carr", "horsepower", "weight", "origin"]
    # Step 4: Create a DataFrame
    df = spark.createDataFrame(data, schema=columns)
    # Step 5: Rename the column 'carr' to 'car'
    df_transformed = df.withColumnRenamed("carr", "car")
    # Step 6: Add the constant column 'AvgWeight' with a value of 200
    df_transformed = df_transformed.withColumn("AvgWeight", lit(200))
    # Step 7: Create a new column 'kilowatt_power' which is 1000 times 'horsepower'
    df_transformed = df_transformed.withColumn("kilowatt_power", col("horsepower") * 1000)
    # Step 8: Show the transformed DataFrame
    df_transformed.show()

if __name__ == '__main__':

    # Step 1: Create a SparkSession
    spark = SparkSession.builder.appName("Car Power Analysis").getOrCreate()

    # Step 2: Sample data
    data = [
        ("Ford Torino", 140, 3449, "US"),
        ("Chevrolet Monte Carlo", 150, 3761, "US"),
        ("BMW 2002", 113, 2234, "Europe")
    ]

car_power_generator(data)

# Step 9: Stop the SparkSession
spark.stop()



