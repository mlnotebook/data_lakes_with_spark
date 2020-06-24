import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates the Spark session with S3 access."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_path, output_path):
    """Processes the song data into `songs` and `artists` parquet tables.
    
    Keyword arguments:
    spark: session
        the spark session.
    input_path: path
        the root directory where the song data is stored.
    output_path: path
        the directory where the parquet tables should be written.
    """
    # get filepath to song data file
    song_data = os.path.join(input_path, 'song_data/*/*/*/*.json')
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration')\
                    .withColumnRenamed('song_id', 's_song_id')\
                    .withColumnRenamed('title', 's_title')\
                    .withColumnRenamed('artist_id', 's_artist_id')\
                    .withColumnRenamed('year', 's_year')\
                    .withColumnRenamed('duration', 's_duration')\
                    .dropDuplicates()
    
    songs_table.createOrReplaceTempView("song_data_table")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .mode('overwrite')\
        .partitionBy('s_year','s_artist_id')\
        .parquet(os.path.join(output_path, 'songs/songs.parquet'))
    
    # extract columns to create artists table
    artists_table = song_df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude')\
                      .withColumnRenamed('artist_name', 'a_name') \
                      .withColumnRenamed('artist_location', 'a_location') \
                      .withColumnRenamed('artist_latitude', 'a_latitude') \
                      .withColumnRenamed('artist_longitude', 'a_longitude') \
                      .dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write\
        .mode('overwrite')\
        .parquet(os.path.join(output_path, 'artists/artists.parquet'))


def process_log_data(spark, input_path, output_path):
    """Processes the log data into `users`, `time`, and `songplays`
    parquet tables.
    
    Keyword arguments:
    spark: session
        the spark session.
    input_path: path
        the root directory where the log data is stored.
    output_path: path
        the directory where the parquet tables should be written.
    """    
    # get filepath to log data file
    log_data = os.path.join(input_path, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')\

    # extract columns for users table    
    users_table = log_df.select('userId',
                                'firstName',
                                'lastName',
                                'gender',
                                'level')\
                    .withColumnRenamed('userId', 'u_userId')\
                    .withColumnRenamed('firstName', 'u_firstName')\
                    .withColumnRenamed('lastName', 'u_lastName')\
                    .withColumnRenamed('gender', 'u_gender')\
                    .withColumnRenamed('level', 'u_level')\
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.createOrReplaceTempView('users')
    users_table.write\
        .mode('overwrite')\
        .parquet(os.path.join(output_path, 'users/users.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select('datetime')\
                    .withColumn('t_start_time', log_df.datetime)\
                    .withColumn('t_hour', F.hour('datetime'))\
                    .withColumn('t_day', F.dayofmonth('datetime'))\
                    .withColumn('t_week', F.weekofyear('datetime'))\
                    .withColumn('t_month', F.month('datetime'))\
                    .withColumn('t_year', F.year('datetime'))\
                    .withColumn('t_weekday', F.dayofweek('datetime'))\
                    .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
        .mode('overwrite')\
        .partitionBy('t_year', 't_month')\
        .parquet(os.path.join(output_path, 'time/time.parquet'))

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_path, "songs/"))\
                .load(os.path.join(output_path, "songs/*/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    song_df = song_df.alias('song_df')
    log_df = log_df.alias('log_df')
    song_log_join = log_df.join(song_df, F.col('log_df.song') == F.col('song_df.s_title'), 'inner')
    songplays_table = song_log_join.select(
        F.col('log_df.datetime').alias('sp_start_time'),
        F.col('log_df.userId').alias('sp_user_id'),
        F.col('log_df.level').alias('sp_level'),
        F.col('song_df.s_song_id').alias('sp_song_id'),
        F.col('song_df.s_artist_id').alias('sp_artist_id'),
        F.col('log_df.sessionId').alias('sp_session_id'),
        F.col('log_df.location').alias('sp_location'), 
        F.col('log_df.userAgent').alias('sp_user_agent'),
        F.year('log_df.datetime').alias('sp_year'),
        F.month('log_df.datetime').alias('sp_month'))\
        .withColumn('sp_songplay_id', F.monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.createOrReplaceTempView('songplays')
    songplays_table.write\
        .mode('overwrite')\
        .partitionBy('sp_year', 'sp_month')\
        .parquet(os.path.join(output_path, 'songplays/songplays.parquet'))


def main():
    """Loads song and log data into parquet tables. Requires the root directory 
    where the two data directories are stored (input_path) and the directory
    where the loaded tables are to be written (output_path).
    """
    spark = create_spark_session()
    
    # For TESTING, set input_path='./data/ and output_path='./'
    input_path = "s3a://udacity-dend/"
    output_path = "s3a://dend-bucket-physrr/"

    input_path = "./data/"
    output_path = "./loaded_tables"
    
    process_song_data(spark, input_path, output_path)    
    process_log_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
