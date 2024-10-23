Problem Statement:

●	Covid-19 data analysis

Use Cases

Section 1
  
●	Rename the column infection_case to infection_source
●	Select only following columns 
○	 'Province','city',infection_source,'confirmed'
●	Change the datatype of confirmed column to integer
●	Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
●	Sort the output in asc order on the basis of confirmed

Section 2
  
●	Return the top 2 provinces on the basis of confirmed cases.

  Section 3
●	Return the details only for ‘Daegu’ as province name where confirmed cases are more than 10
●	Select the columns other than latitude, longitude and case_id

drop_list = ['latitude', 'longitude', 'case_id',]
df = df.select([column for column in df.columns if column not in drop_list])
	
Submission
●	Pyspark code


Solution:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum , max

def covid19_data_analysis(filepath):
    # Load data from a csv file in hdfs to create a dataFrame
    df = spark.read.option("header", True).csv(filepath)

    # USE CASES

    # Section 1: Transformations and Aggregations

    # Rename the column infection_case to infection_source
    df1 = df.withColumnRenamed("infection_case", "infection_source")

    # Select only specific columns 'Province','city','infection_source','confirmed'
    df2 = df1.select('Province', 'city', 'infection_source', 'confirmed')

    # Change the datatype of confirmed column to integer
    df3 = df2.withColumn("confirmed", col("confirmed").cast("int"))

    # Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
    aggregated_df = df3.groupBy("Province", "city").agg(
        sum("confirmed").alias("TotalConfirmed"),
        max("confirmed").alias("MaxFromOneConfirmedCase")
    )

    # Sort the output in asc order on the basis of confirmed
    sorted_aggregated_df = aggregated_df.orderBy("TotalConfirmed", ascending=True)

    print("Section 1 Output:")
    sorted_aggregated_df.show()

    # Section 2

    # Return the top 2 provinces on the basis of confirmed cases
    top_2_provinces = aggregated_df.select("Province", "TotalConfirmed").orderBy("TotalConfirmed",ascending=False).limit(2)
    
    print("Section 2 Output:")
    top_2_provinces.show()

    # Section 3

    # Return the details only for ‘Daegu’ as province name where confirmed cases are more than 10
    daegu_df = df.filter((col("Province") == "Daegu") & (col("confirmed") > 10))
    # Select the columns other than latitude, longitude and case_id
    # Define list of columns to drop: ['latitude', 'longitude', 'case_id']
    drop_list = ['latitude', 'longitude', 'case_id']
    # Select columns not in the drop list
    df4 = df.select([column for column in df.columns if column not in drop_list])

    print("Section 3 Output:")
    df4.show()


# Call function covid19_data_analysis
#Initialize spark session
spark = SparkSession.builder.appName("Covid-19 Data Analysis").getOrCreate()
filepath = "/data/spark/spark_class_project/covid19/covid_cases"
covid19_data_analysis(filepath)

spark.stop()

Terminal output of the code:

takeo@edd038a1be3f:~/PycharmProjects/pythonProject2$ spark-submit pyspark_covid19_data_analysis_class_project.py 
24/10/24 03:08:10 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Section 1 Output:
+----------------+---------------+--------------+-----------------------+
|        Province|           city|TotalConfirmed|MaxFromOneConfirmedCase|
+----------------+---------------+--------------+-----------------------+
|           Seoul|     Gangseo-gu|             0|                      0|
|    Jeollanam-do|from other city|             1|                      1|
|         Jeju-do|from other city|             1|                      1|
|          Sejong|from other city|             1|                      1|
|    Jeollanam-do|       Muan-gun|             2|                      2|
|           Seoul|Yeongdeungpo-gu|             3|                      3|
|Gyeongsangnam-do|     Yangsan-si|             3|                      3|
|      Gangwon-do|       Wonju-si|             4|                      4|
|         Daejeon|from other city|             4|                      2|
|           Daegu|from other city|             4|                      2|
|           Busan|         Jin-gu|             4|                      4|
|           Seoul|   Seodaemun-gu|             5|                      5|
|         Gwangju|        Dong-gu|             5|                      5|
|           Seoul|      Seocho-gu|             5|                      5|
|           Busan|     Suyeong-gu|             5|                      5|
|           Busan|    Haeundae-gu|             6|                      6|
|           Seoul|   Geumcheon-gu|             6|                      6|
|    Jeollabuk-do|from other city|             6|                      3|
|Gyeongsangnam-do|Changnyeong-gun|             7|                      7|
|Gyeongsangnam-do|    Changwon-si|             7|                      7|
+----------------+---------------+--------------+-----------------------+
only showing top 20 rows

Section 2 Output:
+--------+--------------+
|Province|TotalConfirmed|
+--------+--------------+
|   Daegu|          4511|
|   Daegu|          1705|
+--------+--------------+

Section 3 Output:
+--------+---------------+-----+--------------------+---------+
|province|           city|group|      infection_case|confirmed|
+--------+---------------+-----+--------------------+---------+
|   Seoul|     Yongsan-gu| TRUE|       Itaewon Clubs|      139|
|   Seoul|      Gwanak-gu| TRUE|             Richway|      119|
|   Seoul|        Guro-gu| TRUE| Guro-gu Call Center|       95|
|   Seoul|   Yangcheon-gu| TRUE|Yangcheon Table T...|       43|
|   Seoul|      Dobong-gu| TRUE|     Day Care Center|       43|
|   Seoul|        Guro-gu| TRUE|Manmin Central Ch...|       41|
|   Seoul|from other city| TRUE|SMR Newly Planted...|       36|
|   Seoul|  Dongdaemun-gu| TRUE|       Dongan Church|       17|
|   Seoul|from other city| TRUE|Coupang Logistics...|       25|
|   Seoul|      Gwanak-gu| TRUE|     Wangsung Church|       30|
|   Seoul|   Eunpyeong-gu| TRUE|Eunpyeong St. Mar...|       14|
|   Seoul|   Seongdong-gu| TRUE|    Seongdong-gu APT|       13|
|   Seoul|      Jongno-gu| TRUE|Jongno Community ...|       10|
|   Seoul|     Gangnam-gu| TRUE|Samsung Medical C...|        7|
|   Seoul|        Jung-gu| TRUE|Jung-gu Fashion C...|        7|
|   Seoul|   Seodaemun-gu| TRUE|  Yeonana News Class|        5|
|   Seoul|      Jongno-gu| TRUE|Korea Campus Crus...|        7|
|   Seoul|     Gangnam-gu| TRUE|Gangnam Yeoksam-d...|        6|
|   Seoul|from other city| TRUE|Daejeon door-to-d...|        1|
|   Seoul|   Geumcheon-gu| TRUE|Geumcheon-gu rice...|        6|
+--------+---------------+-----+--------------------+---------+
only showing top 20 rows

takeo@edd038a1be3f:~/PycharmProjects/pythonProject2$ 
