import configparser               # Parse configuration file
import psycopg2                   # PostgreSQL database adapter for the Python
import sys                        # Used for exiting python script in case of error
#from no_op_sql_queries import create_table_queries, drop_table_queries      #Non-Optimized DWH Tables
from sql_queries import create_table_queries, drop_table_queries    # SQL query definitions

"""
Purpose:
    Loops through drop_table_queries list and runs drop table queries
    drop_table_queries are defined in sql_queries file
Arg:
    cur - Redshift connection cursor [Required]
    conn - Redshift connection [Required]
"""
def drop_tables(cur, conn):
    num_queries = len(drop_table_queries)
    query_count = 0
    for query in drop_table_queries:
        query_count += 1
        print("Running ", query_count, "/", num_queries, " drop table queries")
        cur.execute(query)
        conn.commit()

"""
Purpose:
    Loops through create_table_queries list and runs create table queries
    create_table_queries are defined in sql_queries file
Arg:
    cur - Redshift connection cursor [Required]
    conn - Redshift connection [Required]
"""
def create_tables(cur, conn):
    num_queries = len(create_table_queries)
    query_count = 0
    for query in create_table_queries:
        query_count += 1
        print("Running ", query_count, "/", num_queries, " create table queries")
        cur.execute(query)
        conn.commit()

"""
Purpose:
    Reads Redshift Cluster connection info stored in dwh.cfg config file
    Connects to cluster using config details and retrieves Connection and Cursor handle
    Upon connecting successfully, calls drop_tables() and create_table() functions
"""
def main():
    config = configparser.ConfigParser()
    # Open and Read config file to retrieve Cluster and DWH details required to connect
    print("Reading 'dwh.cfg' Config file...")
    try:
        config.read('dwh.cfg')
        print("Complete reading 'dwh.cfg' Config file")
    except Exception as e:
        print("Error reading Config file: ", e)
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

    # Running Drop Table queries
    print("Running Drop Table queries...")
    try:
        drop_tables(cur, conn)
        print("Drop table queries complete")
    except Exception as e:
        print("Error dropping tables: ", e)
        print("Closing connection to data warehouse...")
        conn.close()
        sys.exit()

    # Running Create Table queries
    print("Running Create Table queries...")
    try:
        create_tables(cur, conn)
        print("Create table queries complete")
    except Exception as e:
        print("Error creating tables: ", e)
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
