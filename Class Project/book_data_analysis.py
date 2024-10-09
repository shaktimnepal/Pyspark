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


