# udacity_project5

## Summary
This project is for using airflow to construct data pipeline from AWS S3 to AWS redshift.The dataset is as the same as before project, Sparkify's dataset.

## Data
Log data: `s3://udacity-dend/log_data`

Song data: `s3://udacity-dend/song_data`

## Prerequisites
1. This project need an AWS redshift cluster to run. You should have your own ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
2. you should create redshift tables first before run the airflow dag. The tables create statement is in the `create_tables.sql`