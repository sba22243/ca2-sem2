from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import time, os

# Parameters

# Topic
topic = "vaccine"

# Read the directory and find the bz2 files containint the tweets
dataset_path = r'/home/mara/datasets/twitter'


# Size of the batch 
num_files = 2

destination_folder = f'/home/mara/datasets/topic'

spark_conf = SparkConf().setAppName("sba22243-step1").set("spark.sql.debug.maxToStringFields", 100)

# create the Spark context
spark_context = SparkContext.getOrCreate(spark_conf)
sqlContext = SQLContext(spark_context)

def filter_tweets(month, filename):
    folder_list = [] # this list contain the folder where the bz2 are located
    # explore the subfolder
    for subdir, dirs, files in os.walk(dataset_path + '/' + month):
        for file in files:
            # if the file is a bz2 then get the fildername
            if file.endswith('.bz2'):
                folder_list.append(subdir + '/*.json.bz2')
                break
                
    print(f'Num of files {len(folder_list)}')
    print(folder_list)

    # Read a batch of files in parallel
    for folder_to_read in folder_list:
        dataframe = sqlContext.read.json('file:///'+folder_to_read)

        # filter the twitter based on the chosen topic
        dataframe_result = dataframe.filter((col('lang') == 'en') & col('text').rlike(f'(?i){topic}'))
        
        # extract only the relevant columns
        dataframe_result = dataframe_result.select("created_at", "text","timestamp_ms")
        
        print(f'writing {dataframe_result.count()} tweets from folder {folder_to_read}')
        dataframe_result.write.mode("append").json('file:///' + destination_folder + '/' + filename + '.json')


filter_tweets('2021/01','vaccine-January-2021')