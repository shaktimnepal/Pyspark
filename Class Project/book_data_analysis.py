Problem Statement:

Books data analysis

Use Cases
Create DF on json
 - df= spark.read.json('books.json')
Counts the number of rows in dataframe
Counts the number of distinct rows in dataframe
Remove Duplicate Values
Select title and assign 0 or 1 depending on title when title is not odd ODD HOURS and assign this value to a column named as newHours

from pyspark.sql.functions import *
df.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0).alias("newHours")).show(10)

Select author and title is TRUE if title has "THE" word in titles and assign this value to a column named as universal
Select substring of author from 1 to 3 and alias as newTitle1

df.select(df.author.substr(1, 3).alias("newTitle1")).show(5)

Select substring of author from 3 to 6 and alias as newTitle2

Show and Count all entries in title, author, rank, price columns
Show rows with for specified authors 
"John Sandford", "Emily Giffin"

Select "author", "title" when 
title startsWith "THE"
title endsWith "IN"
Update column 'amazon_product_url' with 'URL'
Drop columns publisher and published_date

df.drop("publisher", "published_date").show(5)

Group by author, count the books of the authors in the groups
Filtering entries of title Only keeps records having value 'THE HOST'


Submission
Pyspark code

Solution:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, broadcast

def book_data_analyzer(spark, filepath):
    df = spark.read.json(filepath)
    # Step 2: Count rows and distinct rows
    print("Total number of rows:", df.count())
    print("Number of distinct rows:", df.distinct().count())

    # Step 3: Remove duplicates
    df_no_duplicates = df.dropDuplicates()

    # Step 4 & 5: Add 'newHours' and 'universal' columns
    df_with_hours = df_no_duplicates.withColumn(
        "newHours", when(col("title") != 'ODD HOURS', 1).otherwise(0)
    ).withColumn(
        "universal", when(col("title").like('%THE%'), True).otherwise(False)
    )

    # Step 6: Substring of author
    df_with_hours.select(
        col("author").substr(1, 3).alias("newTitle1"),
        col("author").substr(3, 4).alias("newTitle2")
    ).show(5)

    # Step 7: Show selected columns
    df_with_hours.select("title", "author", "rank", "price").show()

    # Step 8: Filter by specific authors
    authors = spark.createDataFrame([("John Sandford",), ("Emily Giffin",)], ["author"])
    df_with_hours.join(broadcast(authors), "author").show()

    # Step 9: Filter titles starting with "THE" or ending with "IN"
    df_with_hours.filter(col("title").rlike("^(THE|.*IN$)")).select("author", "title").show()

    # Step 10: Rename column
    df_renamed = df_with_hours.withColumnRenamed("amazon_product_url", "URL")

    # Step 11: Drop columns
    df_final = df_renamed.drop("publisher", "published_date")
    df_final.show(5)

    # Step 12: Group by author and count books
    df_final.groupBy("author").count().show()

    # Step 13: Filter by title
    df_final.filter(col("title") == "THE HOST").show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Books Data Analysis").getOrCreate()
    filepath = 'file:///home/takeo/data/book_sales_analysis'
    book_data_analyzer(spark, filepath)
    spark.stop()


