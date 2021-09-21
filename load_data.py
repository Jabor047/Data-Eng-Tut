import pandas as pd
import re
import pyspark
from pyspark.sql import Row, SparkSession

def text_to_df(filename: str) -> pyspark.sql.DataFrame:
    lines = open(filename).readlines()

    spark = SparkSession.builder.appName("").getOrCreate()

    return spark.createDataFrame(parse_lines(lines))


def parse_line(id: int, line: str) -> pyspark.sql.Row:
    split = re.compile()
    Line = Row()

    split_str = "\s*," * 24 + "\s*"
    split = re.compile(split_str)
    tokens = split.split(line.strip())

    return (Line(id, tokens[0], ))

def parse_lines(lines: list) -> list:
    return [parse_line(i, x) for i, x in enumerate(lines)]


filename = "I80_davis.txt"
lines = open(filename).readlines(1)
# print(len(lines))
for line in lines:
    print(line.split(","))
    line = line.rstrip()
    print(line.split(",")[:-1])
# print(lines[0].split())


# text_to_df(filename)