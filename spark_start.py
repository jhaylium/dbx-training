from pyspark.sql import SparkSession
import os

"""
These is how you import the types that make up schema
This is how Ian defines tables.

Check and see if this sort of thing exists for Pandas as well.

There are types for each of the common datatypes

"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
Every spark job will start with a Spark session
"""

print(os.environ.get('PYSPARK_PYTHON'))
print(os.environ.get('PYSPARK_DRIVER_PYTHON'))
print(os.environ.get('SPARK_HOME'))
"""
These environments are important in spark and do not get set when you install pyspark.
I think if you use environments you may need to have these get set for the new environemnt.

so far we should be looking in env/ 

There should be a file called conf/spark-env.cmd where spark is installed
"""


python_path = r'C:\Users\jeff\Documents\Coding_Playground\python\dbx-training\env\Scripts\python.exe'
spark_home = r'C:\Users\jeff\Documents\Coding_Playground\python\dbx-training\env\lib\site-packages\pyspark'

os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
os.environ['SPARK_HOME'] = spark_home
os.environ['HADOOP_HOME'] = r'C:\Users\jeff\Documents\hadoop'

spark = (
    SparkSession
    .builder
    .appName("Election Data")
    .master("local[*]")
    .getOrCreate()
    )


emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"
tbl = spark.createDataFrame(emp_data, emp_schema)
print(tbl.rdd.getNumPartitions())

tbl.show()
tbl_final = tbl.where("salary > 50000")

"""
Saving with spark runs into issues with posix when you are on windows
something needs to happen here to help with Hadoop being installed
Hadoop home is somehow the key here.
"""
# tbl_final.write.format("csv").save('files/data/employee_data_wt.csv')

tbl_final.printSchema()
# tbl_final.schema()

"""
Schema's in spark are StructTypes. This is what Ian uses to define his tables. 
You would want to use a class that can map to these struct types and other database 
types to act as an ORM of sorts to allow for portability of your workflows.

Python allows us to be agnostic but so would C or C++ or any language for that matter.

"""

# Columns and

"""
Why do columns matter?
    You'll need to use columns when you query a dataframe. Very similar to what you would do in SQL.
    col, expression, and table.column all do the same thing.
    Expressions allow us to manipulate the columns within a dataframe
        Think casting which is done on a column
        I think you may be able to do mathematical operations using expr as well.
    
    Is a dataframe somehow different from the spark session?
"""
from pyspark.sql.functions import col, expr

print(tbl.salary, type(tbl.salary))
print(tbl['salary'], type(tbl['salary']))
print(expr("salary"), type(expr("salary")))
print(col("salary"), type(col("salary")))

# tbl_filtered = tbl.select((col("employee_id"), expr("name"), tbl.age, tbl.salary))

"""This is a select statement"""
tbl_filtered = tbl.select(col("employee_id"), expr("name"), tbl.age, tbl.salary)
tbl_filtered.show()

"""
This is how we write a basic select statment in spark
"""
tbl_casted = tbl_filtered.select(expr("employee_id as emp_id"), tbl.name, expr("cast(age as int) as age"), tbl.salary)

"""
Select Expression allows you to select and not have to explicitly wrap each expression in an expression method call.
just seems to let us be less verbose.

"""
tbl_slct_cast = tbl_filtered.selectExpr("employee_id as emp_id", "salary", "name", "cast(age as int) as age"
                                        , "salary")
tbl_slct_cast.printSchema()
tbl_slct_cast.show()

tbl_cast_final = tbl_slct_cast.where(tbl_slct_cast.age > 30)
tbl_cast_final.show()
