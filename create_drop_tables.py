import configparser
import psycopg2
import os

from sql_statements import create_table_queries, drop_table_queries

def create_tables(cur, conn):
    """
  Description: The function create the tables in redshift.
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()   

        
def drop_tables(cur, conn):
    """
  Description: The function drops the tables in redshift.
    """

    for query in drop_table_queries:
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
    config.read('dl.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#    drop_tables(cur, conn) #optional
    
    create_tables(cur, conn)
                   
    conn.close()
    


if __name__ == "__main__":
    main()