#!/anaconda3/bin/python
#-----------------------------------------------------
# Assignment 5 (GraphFrames)
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Lasya Pathuri
#-------------------------------------------------------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: assignment_5.py  <input-file.txt>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Assignment_5")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    input_file = sys.argv[1]

    # Read textfile into dataframe
    df = spark.read.text(input_file)

    # Dropping rows that contain character '#'
    df_2 = df.filter("value not like '%#%'")

    # df_2 is created as a one-column dataframe with data that belongs in 2 columns
    # The function below takes that column and splits the values by '\t',
    # Taking all distinct values found in that column to create a new dataframe
    # Renaming the column name as "id"
    vertices = df_2.withColumn('value', explode(split('value','\t'))).distinct().selectExpr("value as id")

    # df_2 is created as one-column dataframe with data that belongs in 2 columns.
    # The function below will split the values within the column into
    # a two-column (source and destination) dataframe 
    edges = df_2.select(split(df_2.value,"\t")).rdd.flatMap(lambda x: x).toDF(schema=["src","dst"])

    # Creating graphframe
    graph = GraphFrame(vertices, edges)

    # Finding all connected components
    spark.sparkContext.setCheckpointDir('/tmp/graphframes_cps')
    result = graph.connectedComponents()

    # Finding the largest connected component
    result.groupBy('component').count().sort(desc("count")).show(1)

    # Finding the smallest connected component
    result.groupBy('component').count().sort(asc("count")).show(1)

    spark.stop()

