# User Search Pipeline

## Overview

The User Search Pipeline project is designed to process and analyze user search data stored in Parquet format. It utilizes Apache Spark for distributed data processing and Python for scripting and data manipulation tasks.

## Project Structure

The project is organized into the following main components:

- **main.py**: Contains the main script to orchestrate the data pipeline.
- **job/**: Directory containing scripts and modules related to data processing.
- **data/**: Directory where input Parquet files are stored.
- **output/**: Directory where output files (such as CSV files) are stored.

## Requirements

To run this project, ensure you have the following dependencies installed:

- Python (>= 3.x)
- Apache Spark
- PySpark
- pandas
- findspark
- dotenv

Install Python dependencies using `pip`:

```bash
pip install findspark pyspark pandas python-dotenv




root
|-- eventID: string (nullable = true)
|-- datetime: string (nullable = true)
|-- user_id: string (nullable = true)
|-- keyword: string (nullable = true)
|-- category: string (nullable = true)
|-- proxy_isp: string (nullable = true)
|-- platform: string (nullable = true)
|-- networkType: string (nullable = true)
|-- action: string (nullable = true)
|-- userPlansMap: array (nullable = true)
| |-- element: string (containsNull = true)
```
