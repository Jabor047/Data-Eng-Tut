from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrame").master("local[10]").getOrCreate()

# The schema is encoded in a string.
schemaList = ['utc_time_id', 'source_id', 'feed_id', 'primary_link_source_flag',
            'samples', 'avg_speed','avg_flow','avg_occ','avg_freeflow_speed', 
            'avg_travel_time', 'high_quality_samples', 'samples_below_100pct_ff',
            'samples_below_95pct_ff', 'samples_below_90pct_ff', 'samples_below_85pct_ff',
            'samples_below_80pct_ff', 'samples_below_75pct_ff', 'samples_below_70pct_ff',
            'samples_below_65pct_ff', 'samples_below_60pct_ff', 'samples_below_55pct_ff',
            'samples_below_50pct_ff', 'samples_below_45pct_ff', 'samples_below_40pct_ff',
            'samples_below_35pct_ff', 'samples_below_30pct_ff']

# cast all columns as String --  to be reformmated while defining schema for mysql dwh
fields = [StructField(field_name, StringType(), True) for field_name in schemaList]
schema = StructType(fields)

df = spark.read.csv("../data_store/I80_davis.txt", header=True, schema=schema)

print(df.show(5))

# Creates a temporary view using the DataFrame
df.createOrReplaceTempView("stations")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT * FROM stations LIMIT 20;")

# print(results.show())
print(df.rdd.getNumPartitions())

# store data as a csv or parquet
def write_out_df(df, format:str):
    df.write.option('header', True).format(format).save("I80_davis_csv")

write_out_df(df, format='csv')

# write_out_df(df, 'parquet')

