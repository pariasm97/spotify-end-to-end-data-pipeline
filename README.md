# Spotify End To End Data Pipeline Project

## Overview

This project focuses on extracting diverse data from the Spotify API, encompassing details such as artists, albums, and songs from a specified playlist and store them in AWS Simple Storage Service (S3), for then to make analysis in AWS Athena. The ultimate goal is to establish an automated pipeline hosted on AWS for data extraction and processing.

The data processing pipeline for this project involves extracting data, transforming it into a compatible format, moving it to AWS S3, and loading it into tables. Each step ensures the data is efficiently processed, organized, and readily available for analysis, enabling informed business decisions.

## Architecture

![1  Spotify Project Data Pipeline](https://github.com/pariasm97/spotify-end-to-end-data-pipeline/assets/118777139/3b46e5bc-0c6f-4ae9-b19e-ba32ffb795e6)

## Tools Used

**Dataset/API**
- Spotify API [Web API](https://developer.spotify.com/documentation/web-api)

**Programming Language**
 - Python

**Amazon Web Services**
- **Amazon Cloudwatch:** used to monitor the performance and health of the data pipeline and create a starting trigger.
- **Amazon Lambda:** used to build a serverless data processing pipeline.
- **Amazon Simple Storage Service (S3):** used as the data lake for the data engineering project.
- **AWS Glue Crawler:** used to discover and catalog data in S3. This metadata can then be used to query the data with Athena. Glue Crawlers can also be used to generate schemas for data, which can be used to ensure that data is processed and stored in a consistent format.
- **AWS Glue Data Catalog:** central repository for metadata, including data stored in S3. Makes it easy to discover, understand, and use the data.
- **Amazon Athena:** serverless interactive query service that makes it easy to analyze data in S3 using standard SQL.

## Project Structure and Process

**1. Extraction from the Spotify API:** The first step in the process was to create a Spotify account on the developer site to obtain the client ID and client secret key required to access the Spotify API. This allowed the system to extract information from any playlist on the app, but the playlist chosen for this case was the Top Global 50 Songs. A Python script was then created on AWS Lambda to extract the information in JSON format and upload it to an AWS S3 bucket in the `raw_data` folder. This was triggered using Amazon CloudWatch on a daily basis.

**2. Data Transformation:** The second Lambda function was triggered whenever a new element was created in the S3 bucket. This script took the raw data and extracted the information for album, artist, and song. It then stored this information in three different CSV files, which were placed in three different folders inside the `transformed_data` folder on S3.

**3.Data Schema:** Three crawlers were created on AWS Glue Crawler, one for each CSV file. These crawlers were used to create the table schema for each entity. This also allowed the creation of three tables that were then available for analysis on Amazon Athena.

**4.Data Analysis:** On Amazon Athena, it was possible to perform various SQL queries to analyze the information that had been obtained.

## Conclusions

- The data processing pipeline is efficient and scalable. The use of AWS Lambda and Amazon S3 allows the pipeline to scale easily to handle large amounts of data.
- The data is well-organized and easy to query. The use of AWS Glue Crawler and Amazon Athena makes it easy to discover and analyze the data.
- The pipeline is reliable and secure. The use of AWS services ensures that the data is processed and stored securely.
- The pipeline is able to transform the raw data into a format that is easy to analyze, such as CSV files.

Overall, the data processing pipeline is a well-designed and implemented solution for extracting, transforming, loading, and analyzing data from the Spotify API.

