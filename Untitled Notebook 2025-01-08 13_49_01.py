# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# COMMAND ----------

def read_from_api(api_url, headers):
    response = requests.get(api_url, headers=headers)
    data = response.json()
    df = spark.createDataFrame(data)
    return df

# COMMAND ----------

api_url = "https://api.example.com/data"
api_headers = {"Authorization": "Bearer <TOKEN>"}
df_api = read_from_api(api_url, api_headers)

# COMMAND ----------

def write_to_storage(df, storage_path, file_format="parquet"):
    df.write.format(file_format).save(storage_path)

# COMMAND ----------

import requests
from pyspark.sql import SparkSession

def read_from_api(api_url, headers=None):
    
    # Make the GET request to the API
    response = requests.get(api_url, headers=headers)
    
    # Check if the response is successful
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from API. Status Code: {response.status_code}, Message: {response.text}")
    
    # Parse the response JSON
    data = response.json()
    
    # Convert JSON data to a Spark DataFrame
    # Assuming data is a list of dictionaries (tabular format)
    if isinstance(data, list):
        df = spark.createDataFrame(data)
    else:
        # Handle cases where the response is a single object (dict)
        df = spark.createDataFrame([data])
    
    return df

# Example usage
if __name__ == "__main__":
    # Example API URL and headers
    api_url = "https://api.example.com/data"
    headers = {
        "Authorization": "Bearer YOUR_ACCESS_TOKEN"
    }

    # Read data from the API
    try:
        df = read_from_api(api_url, headers)
        df.show()  # Display the DataFrame content
    except Exception as e:
        print(f"Error: {e}")

