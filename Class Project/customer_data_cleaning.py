Customer Data Cleaning

Problem Statement:

As a data engineer you are given the task to clean the customer data
Your pipeline should remove all the duplicates records
And also remove those records which are duplicated on the basis of height and age

Data

Smith,23,5.3
Rashmi,27,5.8
Smith,23,5.3
Payal,27,5.8
Megha,27,5.4

Metadata- columns

Name - String
Age - Integer
Height - double

Solution:

Python code to solve and generate the output for above problem is:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def customer_data_cleaner(data):
    # Step 3: Define column names
    columns = ["Name", "Age", "Height"]
    # Step 4: Create a DataFrame
    df = spark.createDataFrame(data, schema=columns)
    # Step 5: Remove exact duplicate rows
    df_no_duplicates = df.dropDuplicates()
    # Step 6: Remove duplicates based on Age and Height
    df_cleaned = df_no_duplicates.dropDuplicates(subset=["Age", "Height"])
    # Step 7: Show the cleaned data
    df_cleaned.show()


if __name__ == '__main__':

    # Step 1: Create a SparkSession
    spark = SparkSession.builder.appName("Customer Data Cleaning").getOrCreate()

    # Step 2: Define the sample data
    data = [
        ("Smith", 23, 5.3),
        ("Rashmi", 27, 5.8),
        ("Smith", 23, 5.3),
        ("Payal", 27, 5.8),
        ("Megha", 27, 5.4)
    ]

    customer_data_cleaner(data)

# Step 8: Stop the SparkSession
spark.stop()
