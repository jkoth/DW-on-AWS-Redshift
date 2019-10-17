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

