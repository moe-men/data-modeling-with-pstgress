# Data Modeling with Postgres

## Purpose of this database
A startup caleld Sparkify has a new music streaming app and it wants to analyse the data they have been collecting on songs and users activities.
Currently, their team don't have an easy way to query their data, which resides in a directory of JSON log files.

The goal of this project is to build a Postgres relational database with tables designed to optimize queries on song play and help the team perform the analysis.

## How to run the Python scripts
To run the python scripts:
1. open the terminal 
2. type **python create_tables.py**
3. hit enter 
4. type **python etl.py**
5. hit enter 

This should cerate the tables and run the etl job to fill the tables

## An explanation of the files in the repository 
The repository contains :
- data/ : a folder conatains all the data files ( log and music data as JSON files).
- create_tables.py : this python file holds the functions that create the database, drop and create the tables.
- etl.ipynb : jupyter notebook that showcase the etl process using small examples.
- README.md
- sql_queries.py : contains all the sql quries needed to build the Postgres database.
- test.ipynb : jupyter notebook for testing the queries and the etl file.
    
    
## Database schema 
The database schema is a star schema includs songplay table as the Fact Table and 4 Dimension Tables ( users , songs , artists , time)