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

from helper_functions import flatten, get_acis_data
from sql_statements import to_redshift_queries



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.debug.maxToStringFields", 20000) \
        .getOrCreate()
    return spark


    

def create_staging(spark,
                   source_delays,
                   staging_flights_buck,
                   staging_dates_buck,
                   staging_airports_buck,
                   staging_final_buck,
                   sdate,
                   edate,
                   wr_mode="overwrite"
                  ):
    """
    Description: the function load the delays data from the Delays dataset in S3, filtering for the dates selected, and queries the Acis web service launching the function get_acis_data defined above, selecting as airports those found in the filtered delays dataset. It then run spark sql queries to create the final tables and stores them to dedicated S3 buckets. 
    
    Arguments:
      spark: spark session
      staging_flights_buck: destination bucket for the flights table
      staging_dates_buck: destination bucket for the dates table
      staging_airports_buck: destination bucket for the airports  table
      staging_final_buck: destination bucket for the final table
      sdate: start date for observations for acis web service
      edate: end date for observations for acis web service
      wr_mode: writing mode for s3, default to overwrite
      Returns: stores csv tables to the buckets 
    """
    
    delays=spark.read.format("csv").options(header="true", inferSchema="true").load(source_delays+"/*.csv")
    
    delays.where(delays.FL_DATE >= to_date(lit(sdate))) \
          .where(delays.FL_DATE <= to_date(lit(edate))) \
          .distinct().na.drop(subset=("FL_DATE","ORIGIN","DEST")) \
          .createOrReplaceTempView("delays_sql")


    flights = spark.sql(
        """
        select distinct 
        OP_CARRIER carrier,
        OP_CARRIER_FL_NUM fl_num,
        ORIGIN,
        DEST,
        CRS_DEP_TIME,
        CRS_ARR_TIME,
        (weekday(FL_DATE)) weekday,
        min(FL_DATE) first_flight_in_ds,
        max(FL_DATE) last_flight_in_ds
        
        FROM
        delays_sql
        
        group by 
        carrier, 
        fl_num,
        ORIGIN,
        weekday,
        DEST,
        CRS_DEP_TIME,
        CRS_ARR_TIME
        """)
    
    dates=spark.sql(
        """
        select distinct 
        FL_DATE date,
        EXTRACT(YEAR FROM FL_DATE) year,
        EXTRACT(MONTH FROM FL_DATE) month,
        EXTRACT(DAY FROM FL_DATE) day,
        EXTRACT(WEEK FROM FL_DATE) week,
        weekday(FL_DATE) day_of_week
        
        FROM
        delays_sql
        """)
    
   
    
    list_airports=spark.sql(
        """
        WITH append_airports
        AS
        (
        SELECT DISTINCT 
        origin
        
        FROM
        delays_sql
        
        UNION
        
        SELECT DISTINCT 
        dest
        
        FROM
        delays_sql
        )
        
        SELECT DISTINCT * 
        
        FROM 
        append_airports
        """).select('origin').rdd.flatMap(lambda x: x).collect()
    
    separator=","
    string_airports=separator.join(list_airports)        
        
    acis_data=get_acis_data(spark,sdate,edate,sids=string_airports)
    acis_data.createOrReplaceTempView("acis_data_sql")
    
    airports=spark.sql(
        """
        SELECT DISTINCT
        name,
        faa,
        state,
        long,
        lat,
        elev
        
        FROM 
        acis_data_sql
        """)

 
    
    final_dataset=spark.sql(
        """
        with delays_day as
        (SELECT 
        
        FL_DATE,
        origin,
        dest,
        sum (arr_delay) sum_delays_arrival,
        sum (weather_delay) sum_delays_weather,
        count (op_carrier_fl_num) qty_flights,
        sum (crs_elapsed_time) scheduled_el_time,
        sum (actual_elapsed_time) actual_el_time,
        sum (actual_elapsed_time)/sum (crs_elapsed_time) actual_vs_sch_time,
        sum (cancelled) sum_cancelled,
        sum (diverted) sum_diverted,
        sum (case when (arr_delay>30) then 1 else 0 end) delays_above_30,
        sum (case when (actual_elapsed_time/crs_elapsed_time>1.15) then 1 else 0 end) delays_above_15perc,
        sum (case when (cancellation_code ="B") then 1 else 0 end) count_weather_canc
        
        FROM delays_sql
        
        group by
        
        FL_DATE,
        origin,
        dest
        ),
        
        acis_data as 
        
        (SELECT
        faa,
        date,
        sum(pcpn) cumulative_pcpn
        
        from acis_data_sql
        
        group by 
        faa,
        date
        )
        
        select distinct
        FL_DATE,
        origin,
        dest,
        sum_delays_arrival, 
        sum_delays_weather,
        qty_flights,
        scheduled_el_time,
        actual_el_time,
        actual_vs_sch_time,
        sum_cancelled,
        sum_diverted,
        delays_above_30,
        delays_above_15perc,
        count_weather_canc,
        acis_data1.cumulative_pcpn as pcpn_dep,
        acis_data2.cumulative_pcpn as pcpn_arr
        
        from 
        delays_day
        
        join acis_data as acis_data1
        
        on delays_day.fl_date=acis_data1.date
        and delays_day.origin=acis_data1.faa
        
        join acis_data as acis_data2
        
        on delays_day.fl_date=acis_data2.date
        and delays_day.origin=acis_data2.faa
        """
    )
    
    
#Quality checks and write to S3
    
    if flights.count()>0:  
        flights.write.mode(wr_mode).csv(staging_flights_buck+"/flights.csv")
    else:
        print("Flights table has 0 entries")

    if dates.count()>0:  
        dates.write.mode(wr_mode).csv(staging_dates_buck+"/dates.csv")
    else:
        print("Dates table has 0 entries")

    if airports.count()>0:  
        airports.write.mode(wr_mode).csv(staging_airports_buck+"/airports.csv")
    else:
        print("Airports table has 0 entries")

    if final_dataset.count()>0:
        final_dataset.write.mode(wr_mode).csv(staging_final_buck+"/final_dataset.csv")
    else:
        print ("Final dataset has 0 entries")
    
    
    
def copy_to_redshift_tables(cur, conn):
    """
  Description: The function copy the data from the staging tables in S3 to redshift.
    """

    for query in to_redshift_queries:
        cur.execute(query)
        conn.commit()
        
    
def main():
    """
      Description: 
         - Read the cfg file defining Redshift (target) cluster information, IAM role, S3 bucket sources and destinations. 
      
         - Establishes connection to the target Redshift cluster through the information in dwh.cfg and gets
    cursor to it.   
    
        - Calls functions to fetch data from S3 and from ACIS web services
        
        - Transform and load data to staging in S3
        
        - Load data from staging in S3 to Redshift
        
        - Close the connection
    """
    
    config = configparser.ConfigParser()
    config.read("dl.cfg")
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()
    
    os.environ["AWS_ACCESS_KEY_ID"]=config["AWS"]["AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"]=config["AWS"]["AWS_SECRET_ACCESS_KEY"]

    
  #  spark=create_spark_session()
    
  #  create_staging(spark,
   #                source_delays = "{}".format(config.get("STAGE","source_delays")),
   #                staging_flights_buck = "{}".format(config.get("STAGE","staging_flights_buck")),
   #                staging_dates_buck = "{}".format(config.get("STAGE","staging_dates_buck")),
   #                staging_airports_buck = "{}".format(config.get("STAGE","staging_airports_buck")),
   #                staging_final_buck = "{}".format(config.get("STAGE","staging_final_buck")),
   #                sdate = "{}".format(config.get("STAGE","sdate")),
    #               edate = "{}".format(config.get("STAGE","edate")))

                   
                 
    copy_to_redshift_tables(cur, conn)
    
    conn.close()
    
    #close spark connection


if __name__ == "__main__":
    main()