from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import datetime
import pytz
import configparser
from function import cleanText
from function import get_subjectivity, get_polarity, get_sentiment
from pyspark.sql.types import StringType, DoubleType
import pandas as pd
config = configparser.ConfigParser()
config.read('configuration.ini')

tweets_raw_file_path = config['data_preprocessing']['tweets_raw_file_path']
tweets_clean_file_path = config['data_preprocessing']['tweets_clean_file_path']
time_zone = config['data_preprocessing']['time_zone']
positive_boundary = float(config['data_preprocessing']['positive_boundary'])
negative_boundary = float(config['data_preprocessing']['negative_boundary'])
method = config['data_preprocessing']['method']


current_time = datetime.datetime.now(pytz.timezone(time_zone))
print('starting time: ' + str(current_time))
current_time_string = current_time.strftime("%Y%m%d-%H%M%S")

tweets_clean_file = tweets_clean_file_path + '/tweets_' + current_time_string


spark = SparkSession \
    .builder \
    .appName("data cleaning and sentiment analysis") \
    .getOrCreate()

#small size file for testing
pandasDF = pd.read_csv(tweets_raw_file_path, index_col=None, header=0, usecols= ['Datetime', 'Tweet Id', 'Text', 'Username' ])
df=spark.createDataFrame(pandasDF) 
df.printSchema()

cleanText_func = udf(lambda x:cleanText(str(x), method),StringType())
df = df.select('Datetime','Text', cleanText_func('Text').alias('Clean_Text'))

if (method == 'textblob'):

    subjectivity_func = udf(lambda x:get_subjectivity(str(x)),DoubleType())
    df = df.select('Datetime','Clean_Text', 'Text', subjectivity_func('Clean_Text').alias('Subjectivity'))

    polarity_func = udf(lambda x:get_polarity(str(x)),DoubleType())
    df = df.select('Datetime','Clean_Text', 'Text', 'Subjectivity', polarity_func('Clean_Text').alias('Polarity'))

    sentiment_func = udf(lambda x:get_sentiment(float(str(x)), float(positive_boundary), float(negative_boundary)),StringType())
    df = df.select('Datetime','Clean_Text', 'Text', 'Subjectivity','Polarity', sentiment_func('Polarity').alias('Sentiment'))
    print(df.show(10))

    df.repartition(1).write.csv(tweets_clean_file)

current_time = datetime.datetime.now(pytz.timezone(time_zone))
print('ending time: ' + str(current_time))

