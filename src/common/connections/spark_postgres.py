from pyspark.sql import SparkSession
from config import settings


spark = (SparkSession
            .builder
            .appName("Postgres Connector")
            .config("spark.jars", "./jars/postgresql-42.7.5.jar")
            .getOrCreate()
         )

jdbcURL = f"jdbc:postgresql://{settings.POSTGRES_HOST}:5432/{settings.POSTGRES_DB}"
connection_props = {"user": "postgres", "password": settings.POSTGRES_PW, "driver": "org.postgresql.Driver"}

qry = 'select * from finance.credit_card_purchases'
df = spark.read \
    .jdbc(url=jdbcURL,
          table=qry,
          properties=connection_props)

df.show()
