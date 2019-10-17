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

