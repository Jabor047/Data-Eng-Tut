from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType

spark = SparkSession.builder.appName("DataFrame").master("local[10]").getOrCreate()

# The schema is encoded in a string.
schemaList = [
    "utc_time_id",
    "station_id",
    "flow1",
    "occupancy1",
    "flow2",
    "occupancy2",
    "flow3",
    "occupancy3",
    "flow4",
    "occupancy4",
    "flow5",
    "occupancy5",
    "flow6",
    "occupancy6",
    "flow7",
    "occupancy7",
    "flow8",
    "occupancy8",
    "flow9",
    "occupancy9",
    "flow10",
    "occupancy10",
    "flow11",
    "occupancy11",
    "flow12",
    "occupancy12",
]

# cast all columns as String --  to be reformmated while defining schema for mysql dwh
fields = [StructField(field_name, StringType(), True) for field_name in schemaList]
schema = StructType(fields)

df = spark.read.csv("../I80_davis.txt", header=True, schema=schema)

print(df.show(5))

# Creates a temporary view using the DataFrame
df.createOrReplaceTempView("stations")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT * FROM stations LIMIT 20;")

# print(results.show())
print(df.rdd.getNumPartitions())

# store data as a csv or parquet
def write_out_df(df, format: str):
    df.write.option("header", True).format(format).save("I80_davis_csv")


write_out_df(df, format="csv")
