# Data Lakes with Spark from S3 Bucket Data.

A project completed with the Udacity Nanodegree Program in Data Engineering.

## Purpose

Sparkify are a new music streaming app. They've been collecting data on the songs in their catalogue as well as user activity. The data are currently found in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs in their app. The size of the datasets has grown considerably and the analytics team wish to understand what songs users are listening to. This ETL pipeline takes data from the Data Lake and loads it into tables saves as parquet files.

## Schema

The tables and their columns are shown in the schema in Figure  1. This is a star schema with one fact table and 4 dimension tables.

**Fact Table**:
* `songplays`

**Dimension Tables**:
* `users`
* `songs`
* `artists`
* `time`

![StarSchema](./images/project3_schema.png#center) 
Figure 1: The Star Schema modeled in this project with one fact table and 4 dimension tables.


## Data

The song and log data locations are:

* Song Data: `s3://udacity-dend/song_data`
* Log Data: `s3://udacity-dend/log_data`

## Usage

0. To access the S3 buckets, your AWS credentials must saved into `dl.cfg` with the format:

```
[AWS]
AWS_ACCESS_KEY_ID=<your key id here>
AWS_SECRET_ACCESS_KEY=<youe secret key here>
```

1. The ETL pipeline may then be run with the command. Runtime is ~ 3 minutes.

```python
python ./etl.py
```

2. A new folder is created which contains the 5 tables, each in their own subdirectory. The
output path can be changed by modifying the `output_path` variable in `etl.py`)

<mark>An example of the pipeline running on spark cluster is shown in `MyDataLakeNotebook.ipynb`.</mark>

## Files

### `etl.py`

* The ETL pipeline loading data from S3 into the `songs`, `artists`, `users`, `time`, and `songplays` tables.
* `process_song_data` processes the song `.json` files.
* `process_log_data` processes the log `.json` files.
    * timestamp components are extracted for the `time` table.


