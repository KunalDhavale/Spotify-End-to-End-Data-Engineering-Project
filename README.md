# Spotify End-To-End Data Engineering Project

### Description
#### In this project, we will build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline will retrieve data from the Spotify API, transform it to a desired format, and load it into an AWS data store.

### Architecture
![Architecture Diagram](https://github.com/KunalDhavale/Spotify-End-to-End-Data-Engineering-Project/blob/master/architecture.png)

### About Dataset/API
This API contains information about the music artist, albums & songs – [Spotify API](https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF)

### Services Used
1. **Amazon S3:** Amazon S3 (Simple Storage Service) is a highly scalable object storage service that can store & retrieve any amount of data from anywhere on the web. It is commonly used to store & distribute large media files, data backups, & static website files.
2. **AWS Lambda:** AWS Lambda is a service that lets you run your code without managing servers. You can use Lambda to run a code in response to events like changes in S3, DynamoDB, or other AWS services.
3. **Cloud Watch:** Amazon CloudWatch is a monitoring service for AWS resources & the application you run on them. You can use CloudWatch to collect and track metrics, which are variables you can measure for your resources and applications.
4. **AWS IAM:** AWS Identity and Access Management (IAM) is a web service that helps you securely control access to AWS resources. With IAM, you can centrally manage permissions that control which AWS resources users can access. This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.
5. **Glue Crawler:** A crawler accesses your data store, extracts metadata, and creates table definitions in the AWS Glue Data Catalog. The Crawlers pane in the AWS Glue console lists all the crawlers that you create. The list displays the status and metrics from the last run of your crawler.
6. **Data Catalog:** The AWS Glue Data Catalog contains references to data that is used as sources and targets of your extract, transform, and load (ETL) jobs in AWS Glue. To create your data warehouse or data lake, you must catalog this data. The AWS Glue Data Catalog is an index of your data's location, schema, and runtime metrics.
7. **AWS Athena:** Amazon Athena is an interactive query service that makes it easy to analyze data directly in Amazon Simple Storage Service (Amazon S3) using standard SQL. With a few actions in the AWS Management Console, you can point Athena at your data stored in S3 and begin using standard SQL to run ad-hoc queries and get results in seconds.

## Technologies Used:

<img src="https://img.shields.io/badge/-Python-blue?style=for-the-badge"> <img src="https://img.shields.io/badge/-Jupyter Notebook-orange?style=for-the-badge"> <img src="https://img.shields.io/badge/-Spotify API-darkgreen?style=for-the-badge"> <img src="https://img.shields.io/badge/-Pandas-purple?style=for-the-badge"> <img src="https://img.shields.io/badge/-AWS-black?style=for-the-badge">

### Install Packages
```
pip install pandas
pip install numpy
pip install spotipy
```
### Project Execution Flow
Extract Data from API –> Lambda Trigger –> Run Extract Code –> Store it Raw Data –> Trigger Transform Function –> Transform Data and Load It –> Query Using Athena



