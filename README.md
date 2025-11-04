# ETL Project – Data Pipeline with MySQL, PostgreSQL and Airflow

## Overview
This project is divided into two stages.  
The first stage implements an ETL (Extract, Transform, Load) pipeline. During the extraction phase, data is pulled from a MySQL database, then transformed, and finally loaded into a PostgreSQL data warehouse.  
In the second stage, the process automates the extraction of information from a .txt file to retrieve the IP addresses of stored transactions and save them into another .txt file. This task runs daily to check for new records.  

The CSV file contains records, along with additional columns such as timestamps representing transaction time.  
**Source** [IBM Skills Networks - sales.csv](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/sales-csv3mo8i5SHvta76u7DzUfhiw.csv)  
This is the original dataset and includes:  
- rows IDs  
- Product IDs  
- Customer IDs
- Price  
- Quantity  
- Timestamp  

The new data is loaded into MySQL database:  
**Source** [IBM Skills Network - sales.sql](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/sales.sql)  
This dataset simulates retail transactions and includes:  
- Row IDs  
- Product IDs  
- Customer IDs  
- Quantity

For the second stage, we have the data:  
**Source** [IBM Skills Networks - accesslog.txt](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt)  
This dataset includes information about transactions including IP addresses, dates and times, URLs, browsers and other details. For this project, only the IP address field is relevant to the task.

## Structure
ETLPROJECT/  
│  
├── airflow/                   # Airflow environment  
│   ├── dags/                  # ETL DAGs definition  
│   ├── logs/                  # Airflow execution logs  
│   └── plugins/               # Custom Airflow plugins  
│  
├── mysql/  
│   ├── data/                  # MySQL data (CSV or SQL dumps)  
│   └── files/                 # Initialization or configuration files  
│  
├── postgres/  
│   ├── data/                  # PostgreSQL data or exports  
│   └── files/                 # Initialization or schema files  
│  
├── scripts/                   # Python automation and configs  
│   ├── automation.py          # Main automation logic  
│   ├── mysql_config.py        # MySQL connection and settings  
│   └── postgres_config.py     # PostgreSQL connection and settings  
│  
├── .env                       # Environment variables (DB credentials, etc.)  
├── .gitignore                 # Files and folders to exclude from Git  
├── airflow.sh                 # Script to initialize/start Airflow  
├── docker-compose.yml         # Container orchestration  
├── requirements.txt           # Python dependencies  
├── setup.sh                   # Script to set up the project environment  
└── README.md                  # Project documentation  

## Project stages

For the initial stage, the process starts by running the setup.sh script. This script performs the initial configuration to create the MySQL and PostgreSQL instances in Docker.  
- In mysql_config.py, the SQL schema file is downloaded to create the MySQL database (sales) and populate the table (sales_data).  
- In postgres_config.py, the CSV file is downloaded and used to create the table (sales_data) within the PostgreSQL database (sales). The same CSV file is used to populate the PostgreSQL data warehouse.  
- In automation.py, new records in MySQL table are identified by comparing them with the initial CSV file. Some new records may have missing information, specifically in the price and timestamp columns.
  * To populate the timestamp column, the script automatically records the date and time when new records are loaded into the PostgreSQL database.  
  * For the price column, the script looks up existing records by matching Product IDs and fills in the missing values accordingly.

As a result, every time the setup.sh script is executed, the datasets are updated, and any new records are automatically processed.

For the final stage, the process starts by running the airflow.sh script. This script sets up the initial configuration to create the Apache Airflow instance in Docker.  
Inside the "./airflow/files" directory, you can find the process_web_log.py script, which contains the DAG responsible for automating the workflow tasks.  
- The first task extracts the IP addresses from the previously saved file "data.txt".  
- The second task transforms the data by filtering out the IP address (198.46.149.143), keeping only the remaining records.  
- The third task loads the transformed data into a compressed file named "weblog.tar".

In this way, the data pipeline is defined as:  
extract_data_task >> transform_data_task >> load_data_task  
This DAG runs daily in Apache Airflow, automating the entire process.

## Technologies Used
- Python  
- MySQL  
- PostgreSQL  
- Apache Airflow  
- Docker & Docker Compose

## License
This project is intended for educational purposes only.  
Dataset © IBM Skills Network.