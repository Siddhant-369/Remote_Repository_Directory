from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from pyspark.sql.types import *
#processing the json data.
#For using cast import like:- from pyspark.sql.types import * . It is a method. not a function.

data="C:\Bigdata\drivers\zips.json"
df=spark.read.format('json').option("mode",'DROPMALFORMED').load(data) #Bcoz we have corrupted records.
#df.show(truncate=False)
#df.printSchema()

"""Observe, the 'loc' column's data type is array, So, i wan't to process this array format then,
first array data you must make structure. How to make structure is as follows,"""
