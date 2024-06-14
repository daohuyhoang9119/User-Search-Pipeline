#Install libraries
import os
from dotenv import load_dotenv
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f


spark = SparkSession.builder.config("spark.driver.memory", "10g").getOrCreate()


def read_data(path, list_file):
    try:
        df = spark.read.parquet(path)
        schema = StructType([
            StructField("datetime", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("keyword", StringType(), True),
            StructField("platform", StringType(), True)
        ])
        data = spark.createDataFrame([],schema = schema)
        for i in list_file:
            df = spark.read.parquet(path+i)
            df = df.select('datetime','user_id','keyword','platform')
            data = data.union(df)
            print('Read data {}'.format(i))
        data = data.filter(data.user_id.isNotNull())
        data = data.filter(data.keyword.isNotNull())
        print('Data read finished')
        return df
    except Exception as e:
        print(f"Error reading data from {path}")
        return None
    
def transform_data(data):
    data = data.select("datetime","user_id","keyword","platform","action")
    data = data.filter(data.user_id.isNotNull())
    data = data.filter(data.keyword.isNotNull())
    return data

def processing_most_search(data):
    most_search = data.select('user_id','keyword')
    most_search = most_search.groupBy('user_id','keyword').count()
    most_search = most_search.withColumnRenamed('count','TotalSearch')
    most_search = most_search.orderBy('user_id',ascending=False)
    window = Window.partitionBy('user_id').orderBy(f.col('TotalSearch').desc())
    most_search = most_search.withColumn('Row_Number',f.row_number().over(window))
    most_search = most_search.filter(most_search.Row_Number == 1)
    most_search = most_search.select('user_id','keyword')
    most_search = most_search.withColumnRenamed('keyword','Most_Search')
    return most_search

def write_to_csv(df,output_path):
    try:
        df.write.mode('overwrite').csv(output_path, header=True)
        print(f"Data successfully written to {output_path}")
    except Exception as e:
        print(f"Error writing data to {output_path}: {e}")

    
def main(path):
    print("---------Reading files from folder--------------")
    list_t6 = os.listdir(path)[0:2]
    df = read_data(path,list_t6)


    print("---------Transform Data--------------")
    # df = transform_data(df)
    df.show(4, truncate=False)

    print("---------convert Data--------------")
    df = processing_most_search(df)
    df.show(5,truncate=False)



    # load_dotenv()
    # output_path = os.getenv("OUTPUT_PATH")
    # print("---------Write to csv--------------")
    # write_to_csv(df, output_path)
    
    
    print("---------Finish--------------")
    spark.stop()


if __name__ == "__main__":
    load_dotenv()
    input_path = os.getenv("INPUT_PATH")
    main(input_path)