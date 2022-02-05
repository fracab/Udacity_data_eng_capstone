import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES

flights_table_drop = "DROP TABLE IF EXISTS flights cascade"
dates_table_drop = "DROP TABLE IF EXISTS dates cascade"
airports_table_drop = "DROP TABLE IF EXISTS airports cascade"
final_dataset_table_drop = "DROP TABLE IF EXISTS final_dataset cascade"

# CREATE TABLES

flights_table_create= ("""
CREATE TABLE IF NOT EXISTS flights (
    carrier varchar(2),
    fl_num int,
    origin varchar(3),
    dest varchar(3),
    crs_dep_time int,
    crs_arr_time int,
    weekday int,
    first_flight_in_ds date,
    last_flight_in_ds date
    );
""")

dates_table_create = ("""
CREATE TABLE IF NOT EXISTS dates (
    FL_DATE date,
    year int,
    month int,
    day int,
    week_of_year int,
    day_of_week int
    );
""")

airports_table_create = ("""
CREATE TABLE IF NOT EXISTS airports (
     name text,
     faa varchar(3),
     state varchar(2),
     long real,
     lat real,
     elev real
     );
""")

final_dataset_table_create = ("""
CREATE TABLE IF NOT EXISTS final_dataset (
    FL_DATE date,
    origin varchar(3),
    dest varchar(3),
    sum_delays_arrival real, 
    sum_delays_weather real,
    qty_flights int,
    scheduled_el_time real,
    actual_el_time real,
    actual_vs_sch_time real,
    sum_cancelled real,
    sum_diverted real,
    delays_above_30 real,
    delays_above_15perc real,
    count_weather_canc real,
    pcpn_dep real,
    pcpn_arr real
    );
""")


#COPY FROM S3 TO REDSHIFT
flights_table_copy = ("""
copy flights 
     from {}
     iam_role '{}'
     CSV 
     DATEFORMAT 'YYYY-MM-DD'
     TIMEFORMAT 'auto'
     compupdate off 
     region \'us-east-1\';
""").format(config.get("S3","staging_flights_file"), config.get("IAM_ROLE", "ARN"))


dates_table_copy = ("""
copy dates 
     from {}
     iam_role '{}' 
     CSV 
     DATEFORMAT 'YYYY-MM-DD'
     TIMEFORMAT 'hhmi'
     compupdate off 
     region \'us-east-1\';
""").format(config.get("S3","staging_dates_file"), config.get("IAM_ROLE", "ARN"))

airports_table_copy = ("""
copy airports 
     from {}
     iam_role '{}' 
     CSV 
     DATEFORMAT 'YYYY-MM-DD'
     TIMEFORMAT 'auto'
     compupdate off 
     region \'us-east-1\';
""").format(config.get("S3","staging_airports_file"), config.get("IAM_ROLE", "ARN"))

final_table_copy = ("""
copy final_dataset 
     from {}
     iam_role '{}' 
     CSV 
     DATEFORMAT 'YYYY-MM-DD'
     TIMEFORMAT 'hhmi'
     compupdate off 
     region \'us-east-1\';
""").format(config.get("S3","staging_final_file"), config.get("IAM_ROLE", "ARN"))



# QUERY LISTS

drop_table_queries = [flights_table_drop, dates_table_drop, airports_table_drop, final_dataset_table_drop]
create_table_queries = [flights_table_create, dates_table_create, airports_table_create, final_dataset_table_create] 
to_redshift_queries = [flights_table_copy, dates_table_copy, airports_table_copy, final_table_copy] 
