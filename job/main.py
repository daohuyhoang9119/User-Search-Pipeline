#Install libraries
import os
from dotenv import load_dotenv
import pandas as pd
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f


spark = SparkSession.builder.config("spark.driver.memory", "10g").getOrCreate()
load_dotenv()
input_path = os.getenv("INPUT_PATH")
output_path = os.getenv("OUTPUT_PATH")


def read_data(path, list_file):
    try:
        schema = StructType([
            StructField("datetime", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("keyword", StringType(), True),
            StructField("platform", StringType(), True)
        ])
        data = spark.createDataFrame([],schema = schema)
        for i in list_file:
            full_path = os.path.join(path, i)
            try:
                df = spark.read.parquet(full_path)
                df = df.select('datetime', 'user_id', 'keyword', 'platform')
                data = data.union(df)
                print(f'Read data {full_path}')
            except Exception as e:
                print(f"Error reading file {full_path}: {e}")
        data = data.filter(data.user_id.isNotNull())
        data = data.filter(data.keyword.isNotNull())
        print('Data read finished')
        return df
    except Exception as e:
        print(f"Error reading data from {path}")
        return None
    
def transform_data(data):
    data = data.select("datetime","user_id","keyword","platform")
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
        # df.write.mode('overwrite').csv(output_path, header=True)
        # print(f"Data successfully written to {output_path}")
        for col in df.columns:
            # Select the column and write it to CSV
            col_df = df.select(col)
            col_output_path = os.path.join(output_path, f"{col}.csv")
            col_df.write.mode('overwrite').csv(col_output_path, header=True)
            print(f"Column '{col}' successfully written to {col_output_path}")
    except Exception as e:
        print(f"Error writing data to {output_path}: {e}")
        

def save_distinct_values(df, column,output_file):
    try:
        distinct_values = df.select(column).distinct().rdd.map(lambda row: row[0]).collect()
        distinct_df = pd.DataFrame(distinct_values, columns=[column])
        
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        distinct_df.to_csv(output_file, index=False, encoding='utf-8')        
        print(f"Distinct values in {column} saved to {output_file}")
    except Exception as e:
        print(f"Error saving distinct values to {output_file}: {e}")


    
def main(path):
    print(path)
    print("---------Reading files from folder--------------")
    list_t6 = os.listdir(path)[0:2]
    print(list_t6)

    list_t7 = os.listdir(path)[2:]
    print(list_t7)


    print("---------Reading data month 6--------------")
    df_6 = read_data(path,list_t6)
    if df_6 is None:
        print("Error reading data for month 6. Exiting.")
        return
    print("---------Reading data month 7--------------")
    df_7 = read_data(path,list_t7)
    if df_7 is None:
        print("Error reading data for month 7. Exiting.")
        return


    print("---------Transform Data month 6 & 7--------------")
    df_6 = transform_data(df_6)
    df_7 = transform_data(df_7)
 
    print("---------Convert Data--------------")
    df_6 = processing_most_search(df_6)
    df_7 = processing_most_search(df_7)
    df_6 = df_6.withColumnRenamed('Most_Search','Most_Search_T6')
    df_7 = df_7.withColumnRenamed('Most_Search','Most_Search_T7')
    df = df_6.join(df_7,'user_id','inner')
    print("---------Result--------------")
    df.show(5,truncate=False)

    # output_folder = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\User Search Pipeline\\output\\month6\\"
    # output_file = os.path.join(output_folder, "distinct_values_Most_Search_T6.csv")
    # save_distinct_values(df, "Most_Search_T6", output_file)

    print("---------Write to csv--------------")
    write_to_csv(df, output_path)
    
    print("---------Finish--------------")
    spark.stop()


if __name__ == "__main__":
    main(input_path)