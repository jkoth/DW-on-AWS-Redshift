# **Relational Database on AWS Redshift**
## **Project Summary**
Mock music app Company, Sparkify's user base and song database has grown significantly and therefore want to move their data on to the Cloud. Goal of the project is to build an ETL pipeline using Python to extract Company's JSON formatted data files from AWS S3 and load them into a Star schema relational database on AWS Redshift, after few required transformations. Key requirement of the project is to optimize the new database for song play analysis.

## **Project Datasets**
### **Songs Dataset**
Subset of the original dataset, Million Song Dataset (Columbia University). Stored on S3 in JSON format containing song metadata. Each file contains metadata for a single song.

### **Log (Events) Dataset**
Dataset is generated using Eventsim simulator hosted on Github and saved on S3 for the project. Files are JSON formatted and each file contains user activity for a single day.

## **Database Design**
### **Star Schema**
Database is designed to contain one Fact table, Songsplay and four Dimension tables, user, songs, artists, and time. Additionally, two Staging tables are created to store Songs and Events data extracted from S3 using COPY command. Multiple SQL queries are built to transform the data from staging tables and insert (bulk insert) result set into database tables.

In order to take advantage of parallel processing on AWS Cloud, tables are distributed using destribution styles best suited for optimizing end-user song play queries. Songplays fact table and song dimension tables are distributed using 'Key' destribution style with song_id as Key, while other dimension tables are distributed using 'ALL' distribution style to reduce data shuffling at query time. To further optimize tables, multiple Sort Keys are assigned to each table which would enable AWS query optimizer to skip blocks of data at run-time. 

## **ETL Process**
ETL is processed using Python scripts that utilizes PostgreSQL module, 'psycopg2' to interact with Redshift. Prior to running the ETL scripts, a Redshift cluster is deployed with the AWS Role that allows Redshift to access S3 resource (read-only). Once the Cluster is active, Cluster host and database details are stored in a configuration file. This file is accessed from within ETL script to connect to the Cluster and process data. 

Data from above mentioned two datasets are extracted from S3 using AWS COPY command and stored in two staging tables on Redshift database. Data in the staging tables are transformed using multiple SQL queries and loaded into one fact table and multiple dimension tables forming a Star schema relational database. 

## **Python Scripts**
Python script is broken down into three parts.  <br>
- “sql_queries.py” defines multiple variables representing SQL statements for CREATE TABLE, DROP TABLE, and INSERT INTO SELECT. Additionally, COPY command is also defined in this file. Paramerters for COPY commands are retrieved from config file using 'ConfigParser' module. These variables are imported in "etl.py" and "create_tables.py".

- “create_tables.py” contains Python functions that connect to Redshift database using cluster details in config file and executes DROP TABLE and CREATE TABLE SQL statements imported from "sql_queries.py".

- “etl.py” contains Python functions that connect to Redshift database using cluster details in config file and executes COPY and INSERT INTO SELECT SQL statements imported from "sql_queries.py".

These files must be updated and processed sequentially. SQL file must be updated with all the SQL code to be used in other two files. Create tables python script must be run before ETL script to make sure required tables are created before inserting data.

## **Example Analytical Queries**
- **Top Ten most played songs:** <br>
    SELECT s.title, count(sp.songplay_id) play_count <br>
    FROM songplays sp, songs s <br>
    WHERE sp.song_id = s.song_id <br>
    GROUP BY s.title <br>
    ORDER BY play_count DESC <br>
    LIMIT 10; <br>
    
- **Top ten locations with most song play count:** <br>
    SELECT sp.location, count(sp.songplay_id) play_count <br>
    FROM songplays sp <br>
    GROUP BY sp.location <br>
    ORDER BY play_count DESC <br>
    LIMIT 10; <br>

- **User counts by Level:** <br>
    SELECT u.level, count(u.user_id) user_count <br>
    FROM users u <br>
    GROUP BY u.level;
