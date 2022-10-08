import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl
from pyspark.sql.types import StringType as Str, IntegerType as Int, DateType as Dat, TimestampType
import  pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create spark session to connect to cluster.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def song_data_schema():
    """
    create data schema to read song data.
    """
    # song data schema
    songSchema = R([
            Fld("num_songs", Int()),
            Fld("artist_id", Str()),
            Fld("artist_latitude", Dbl()),
            Fld("artist_longitude", Dbl()),
            Fld("artist_location", Str()),
            Fld("artist_name", Str()),
            Fld("song_id", Str()),
            Fld("title", Str()),
            Fld("duration", Dbl()),
            Fld("year", Int()),
        ])
    return songSchema

def process_song_data(spark, input_data, output_data):
    """
    extracting and saving song data into tow tables songs and artists.
    """
    # get filepath to song data file
    song_data = input_data+'song_data/'+'A/B/*/*.json'
    
    print("Reading Song Data...")
    # song data schema
    songSchema = song_data_schema()
    
    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE', schema=songSchema).dropDuplicates()
    print("Data is Ready.")
        
    # extract columns to create songs table
    print("Extractings song_table Data...")
    songs_table = df.select(['song_id', 'title', 'artist_id','year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing Song_table Data to S3...")
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+'songs-table/')

    # extract columns to create artists table
    print("Extractings artists_table Data...")
    artists_table =  df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    print("Writing artists_table Data to S3...")
    artists_table.write.parquet(output_data+'artists-table/')


def process_log_data(spark, input_data, output_data):
    """
    extracting and saving log data into tow tables users and time in addtion to the fact table songPlay.
    """
    # get filepath to log data file
    log_data = input_data+'log_data/'+'2018/*/*.json'

    # read log data file
    print("Reading Log Data...")
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    print("Extractings users_table Data...")
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).drop_duplicates()
    
    # write users table to parquet files
    print("Writing users_table Data to S3...")
    users_table.write.parquet(output_data+'users-table/')

    # create timestamp column from original timestamp column
    print("Extractings time_table Data...")
    # get_timestamp = udf()
    df_time_table = df.withColumn("start_time", F.to_timestamp(col("ts")/ 1000 ))
    df_time_table  = df_time_table.select("start_time")
    
    # extract columns to create time table
    time_table = df_time_table.withColumn('hour', F.hour("start_time"))\
                            .withColumn('day', F.dayofmonth("start_time"))\
                            .withColumn('week', F.weekofyear("start_time"))\
                            .withColumn('month', F.month("start_time"))\
                            .withColumn('year', F.year("start_time"))\
                            .withColumn('weekday', F.dayofweek("start_time")) 
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table Data to S3...")
    time_table.write.partitionBy("year","month").parquet(output_data+'time-table/')

    # read in song data to use for songplays table
    print("Reading Song Data for songpaly table...")
    songSchema = song_data_schema()
    song_df = spark.read.json(input_data+'song_data/'+'A/B/*/*.json', mode='PERMISSIVE', schema=songSchema).dropDuplicates() 

    # extract columns from joined song and log datasets to create songplays table 
    print("Extractings songplays_table Data...")
    songplays_table =  song_df.join(df,[df.song ==song_df.title, df.artist==song_df.artist_name, df.length==song_df.duration],"inner")\
                           .select(F.to_timestamp(col("ts")/ 1000 ).alias("start_time"),col("userId"),col("level") ,col("song_id") ,col("artist_id") ,col("sessionId" ),col("location" ),col("userAgent")) 
    
    songplays_table =  songplays_table.join(time_table,time_table.start_time == songplays_table.start_time,"inner")\
                           .select(songplays_table.start_time ,col("userId"),col("level") ,col("song_id") ,col("artist_id") ,col("sessionId" ),col("location" ),col("userAgent") ,col("year" ),col("month") ) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data +'songplays-table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-d-l/sparkfiy-dwh/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
