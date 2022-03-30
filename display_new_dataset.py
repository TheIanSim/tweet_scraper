import pandas as pd
import configparser
config = configparser.ConfigParser()
config.read('configuration.ini')

#read in the dataset file
file = config['display_new_dataset']['dataset_file']

df = pd.read_csv(file, index_col=None, lineterminator='\n', header=None, names= ['Datetime','Clean_Text','Text','Subjectivity','Polarity','Sentiment'], dtype='unicode')
df = df.astype( dtype={
                'Datetime' : str,
                'Clean_Text': str,
                'Text': str,
                'Subjectivity': float,
                'Polarity': float,
                'Sentiment': str})


print("The first 1 record")
print(df.head(1))
print("The last 1 record")
print(df.tail(1))
print("total number of records:")
print(len(df))

