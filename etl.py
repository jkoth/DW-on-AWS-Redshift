import configparser             # Parse configuration file
import psycopg2                 # PostgreSQL database adapter for the Python
import sys                      # Used for exiting python script in case of error
from sql_queries import copy_table_queries, insert_table_queries            # SQL query definitions

"""
Purpose:
    Loops through copy_table_queries list and runs COPY queries
    copy_table_queries are defined in sql_queries file
Arg:
    cur - Redshift connection cursor [Required]
    conn - Redshift connection [Required]
"""
def load_staging_tables(cur, conn):
    num_queries = len(copy_table_queries)
    query_count = 0
    for query in copy_table_queries:
        query_count += 1
        print("Running ", query_count, "/", num_queries, " COPY table queries")
        cur.execute(query)
        conn.commit()

"""
Purpose:
    Loops through insert_table_queries list and runs insert queries
    insert_table_queries are defined in sql_queries file
Arg:
    cur - Redshift connection cursor [Required]
    conn - Redshift connection [Required]
"""
def insert_tables(cur, conn):
    num_queries = len(insert_table_queries)
    query_count = 0
    for query in insert_table_queries:
        query_count += 1
        print("Running ", query_count, "/", num_queries, " INSERT table queries")
        cur.execute(query)
        conn.commit()

"""
Purpose:
    Reads Redshift Cluster connection info stored in dwh.cfg config file
    Connects to cluster using config details and retrieves Connection and Cursor handle
    Upon connecting successfully, calls load_staging_tables() and insert_tables() functions
"""
def main():
    config = configparser.ConfigParser()    
    # Open and Read config file to retrieve Cluster and DWH details required to connect
    print("Reading 'dwh.cfg' Config file...")
    try:
        config.read('dwh.cfg')
        print("Complete reading 'dwh.cfg' Config file")
    except Exception as e:
        print("Error reading config file: ", e)
        sys.exit()

    # Creating a connection using Cluster and DWH details stored in config file
    print("Connecting to Data Warehouse...")
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        print("Connected to Data Warehouse")
    except Exception as e:
        print("Error connecting Data Warehouse: ", e)
        sys.exit()

    # Getting conection cursor
    print("Getting Cursor")
    try:
        cur = conn.cursor()
        print("Connection cursor retrieved")
    except Exception as e:
        print("Error getting connection cursor: ", e)
        print("Closing connection to data warehouse...")
        conn.close()
        sys.exit()
    
    # Executing COPY commands defined in sql_queries file
    print("Loading staging tables")
    try:
        load_staging_tables(cur, conn)
        print("Loading staging tables complete")
    except Exception as e:
        print('Error loading staging table: ', e)
        print("Closing connection to data warehouse...")
        conn.close()
        sys.exit()
        
    # Executing INSERT commands defined in sql_queries file
    print("Inserting rows into DWH tables from staging tables")
    try:
        insert_tables(cur, conn)
        print("Inserting rows into DWH tables complete")
    except Exception as e:
        print('Error inserting data in DWH: ', e)
        print("Closing connection to data warehouse...")
        conn.close()
        sys.exit()

    # Closing connection
    print("Closing connection to data warehouse...")
    conn.close()

"""
    Run above code if the file is labled __main__
      Python internally labels files at runtime to differentiate between imported files and main file
"""
if __name__ == "__main__":
    main()
