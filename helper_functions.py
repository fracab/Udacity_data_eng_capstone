import pandas as pd
import zipfile
import gzip
import glob
from datetime import datetime, timedelta
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, lit, rank, to_date, sequence, regexp_extract, concat_ws, udf, col, explode, explode_outer, year, month, dayofmonth, hour, weekofyear, date_format, split, expr,array_contains, row_number
from pyspark.sql.window import Window
from pyspark.sql import types as T
import json
import urllib.request
from urllib.request import urlopen
import requests
import psycopg2
import time
import boto3
from ast import literal_eval

def flatten(schema, prefix=None):
    
    """
    Description: this is an helper function to transform the JSON output of ACIS web services into a tabular format. It expand an array into several columns following the JSON schema. Credit to https://stackoverflow.com/a/50156142
    Arguments:
    schema: schema of the JSON file 
    prefix: field prefix
    """
    
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, T.ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, T.StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

    
    
def get_acis_data(spark,
                  sdate,
                  edate,
                  sids
                 ):
    """
    Description: the function queries the MultiStn ACIS web service in order to get daily precipitation measured collected in a number of airport stations across the United States identified by their FAA code. The function then transforms the JSON file in tabular format and stores it as parquet file partitioned by year and station in a given destination. Given the size of the dataset in object, the function run in Spark.
    Arguments:
      spark: spark session
      sdate: start date for observations
      edate: end date for observations
      output: output path
      sids: string with the FAA ids to be retrieved
    Returns: Parquet file of the observations
    """

#query the web service and collect the JSON file
    url='http://data.rcc-acis.org/MultiStnData?&sdate='+sdate+'&edate='+edate+'&elems=pcpn&sids='+sids+'&output=json&meta=name,state,ll,uid,elev,sids'
    print(url)
    jsonData = urlopen(url).read().decode('utf-8')

#read the JSON in Spark    
    rdd = spark.sparkContext.parallelize([jsonData])
    to_explode =spark.read.option("multiline","true").json(rdd)

#explode the JSON into a row for each observation
    exploded=to_explode.select(explode(to_explode.data))

#expand the data array into several columns with the previously defined helper function
    flattened=exploded.select(flatten((exploded.schema)))
    
#store longitude and latitude into different columns and extract the FAA identifier
    flattened=flattened.select("name",
                               "state",
                               "sids",
                               concat_ws(", ","sids").alias("sids_string"),
                               "uid",
                               flattened.ll[0].alias("long"),
                               flattened.ll[1].alias("lat"),
                               "elev",
                               "data") \
                        .withColumn("faa",regexp_extract('sids_string', "(\w+(?=\s+3))", 1))                           
    
# filter out any values where the faa identifier is missing and explode the measurements array    
    flattened=flattened.where(flattened.faa != "") \
                       .select("name",
                               "state",
                               "sids",
                               "faa",
                               "uid",
                               "long",
                               "lat",
                               "elev",
                               explode_outer(flattened.data).alias("measure")) \
                       .withColumn('s_date',lit(sdate)) 
    
    
#Partition the dataset by the ACIS dataset ID and assign a date for each entry, stores the dataset as parquet


    w = Window().partitionBy('uid').orderBy("s_date")
    
    flattened = flattened.withColumn('row_id',row_number().over(w)) \
                       .withColumn('date',expr("date_add(s_date, row_id-1)")) \
                       .withColumn('year', date_format(col('date'),'Y')) \
                       .withColumn('month', date_format(col('date'),'M')) \
                       .withColumn('day', date_format(col('date'),'D'))
    
    
    acis_data=flattened.select("name",
                               "faa",
                               "state", 
                               "long", 
                               "lat", 
                               "elev", 
                               "date",
                               "year",
                               "month",
                               "day",
                               flattened.measure[0].alias("pcpn")) 
    
    return acis_data
    #acis_data.write.partitionBy("year","faa").mode("{}".format(mode)).parquet(output+"/acis.parquet") #alternative version to store the file to a destination, not implemented in the current solution
