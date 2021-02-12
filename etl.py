import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, \
                                StringType as Str, IntegerType as Int, DateType as Dat, \
                                TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    # read song data file

    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    print("creating {} at {}".format('songs_table', str(datetime.now())))

    song_cols= ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_cols).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print("writing {} to s3 at {}".format('songs_table', str(datetime.now())))

    songs_table.write.partitionBy('year', 'artist_id')\
    .parquet(os.path.join(output_data,'song'),'overwrite')
    
    # extract columns to create artists table
    print("creating {} at {}".format('artists_table', str(datetime.now())))

    artist_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_lattitude', 'artist_longitude']
    artists_table =  df.select(artist_cols).drop_duplicates()
    
    # write artists table to parquet files
    print("writing {} to s3 at {}".format('artists_table', str(datetime.now())))
    artists_table.write.parquet(os.path.join(output_data , 'artist'),'overwrite')
    


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data  = os.path.join(input_data, 'log_data/*.json')


    # read log data file
    df  = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    print("creating {} at {}".format('user_table', str(datetime.now())))

    user_col = ["userId","firstName","lastName","gender","level"]
    users_table = df.select(user_col).drop_duplicates()
    
    # write users table to parquet files
    print("writing {} to s3 at {}".format('user_table', str(datetime.now())))

    users_table.write.parquet(os.path.join(output_data , 'user'),'overwrite')

    # create timestamp column from original timestamp column
    print("creating {} at {}".format('time_table', str(datetime.now())))

    df = df.withColumn("timestamp", F.to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", F.to_date(df.timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr(["timestamp as start_time ",
                                "hour(timestamp) as hour",
                                "weekofyear(timestamp) as week",
                                "month(timestamp) as month",
                                "year(timestamp) as year",
                                "dayofweek(timestamp) as weekday"])
    
    # write time table to parquet files partitioned by year and month
    print("writing {} to s3 at {}".format('time_table', str(datetime.now())))

    time_table.write.partitionBy('year', 'month')\   .parquet(os.path.join(output_data,'time_table'),'overwrite')
    


    # read in song data to use for songplays table
    
    song_path = os.path.join(output_data,'song')
    df_song = spark.read.parquet(song_path) 

    # extract columns from joined song and log datasets to create songplays table 
    df_songplay_all = df.join(df_song, (df.song == df_song.title))
    songplay_cols = ["song_id as songplay_id", 
                 "timestamp as start_time " ,
                 'userId as user_id',
                 'level', 
                 'song_id', 
                 'artist_id', 
                 'sessionId as session_id', 'location', 'userAgent as user_agent']
    print("createing {} at {}".format('songplays_table', str(datetime.now())))

    songplays_table =  df_songplay_all.selectExpr(songplay_cols)

    # write songplays table to parquet files partitioned by year and month
     print("writing {} to s3 at {}".format('songplays_table', str(datetime.now())))
    songplays_table.write.partitionBy('year', 'month')\   .parquet(os.path.join(output_data,'songplay'),'overwrite')
    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-nasim/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
