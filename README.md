# Data Lake with Pyspark

The two datasets that reside in S3. Here are the S3 links for each:

    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
    
The idea is to create a star schema using the the two data sets the reside in S3. Then each table will be written to its corresponding table in s3://dend-nasm/

# Fact table

The fact table consist of songplays. Values included in this table are as follow :
    songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dimension tables

The dimension tables include data about artist, song , time and user.

    Each table consist of the folowing : </p>
    -users - users in the app
        user_id, first_name, last_name, gender, level
    -songs - songs in music database
        song_id, title, artist_id, year, duration
    -artists - artists in music database
        artist_id, name, location, lattitude, longitude
    -time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

# Project template

To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on yproject and submit your work through this workspace.


The project template includes three files:

- etl.py is where I have loaded data from S3 and create the tables according to the star schema.
- README.md is where I have provided my process and decisions for this ETL pipeline.
- df.cfg "not included in repo , please put your aws key and acces key here"

# How to run the project

Please run

    python etl.py

From your terminal . the rest would be done automatically.

# Enviroment

    Python 3.6.3
    

