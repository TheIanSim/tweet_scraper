import csv 
import re, string
import nltk
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
nltk.download('stopwords')
nltk.download('vader_lexicon')
nltk.download('wordnet')
from textblob import TextBlob

def read_csv_to_list(file_name):
    data_list = []
    with open(file_name, 'r', newline='') as f:
        for row in csv.reader(f):
            data_list.append(row[0])
    return data_list

def write_header(file_name, header_list):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header_list)

def append_content(file_name, content):
    with open(file_name, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(content)

def cleanText(text, method):
    #Removing @mentions
    text = re.sub('@[A-Za-z0–9]+', '', text)

    #hashtags
    text = re.sub('#[A-Za-z0–9]+', '', text) 

    # Removing RT
    text = re.sub('RT[\s]+', '', text) 

    # Removing hyperlink
    text = re.sub('https?:\/\/\S+', '', text)
    text = re.sub('www.\S+', '', text)

    # removing new line
    text = re.sub('\n', ' ', text)

    # removing special characters
    text = re.sub('&amp;', '', text) 

    # Removing punctuations !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
    punctuation_pattern = re.compile('[%s]' % re.escape(string.punctuation))
    text = punctuation_pattern .sub('', text)

    # removing words with digits
    text = re.sub('[A-Za-z]*\d+|\d+[A-Za-z]*', '', text)

    #Removing emojis - if we use nltk
    if (method == 'nltk'):
        text = text.encode('ascii', 'ignore').decode('UTF-8')

    # convert text to lowercase 
    words_list = text.lower().split()

    # remove stop words
    stop = set(stopwords.words('english'))
    words_list = [word for word in words_list if word not in stop] 

    # word normalization - lematization
    lemmatizer = WordNetLemmatizer()
    words_list = [lemmatizer.lemmatize(word) for word in words_list] 

    text = " ".join(words_list)

    # removing all the single characters
    noise_free_text = re.sub(' [a-zA-Z] ', ' ', text)

    return noise_free_text


#functions for nltk
def sentiment_score_analysis(text):
    return SentimentIntensityAnalyzer().polarity_scores(text)

def sentiment_type_analysis(score, positive_boundary, negative_boundary):
    if score > positive_boundary:
        sentiment_type = 'POSITIVE'
    elif score < negative_boundary:
        sentiment_type = 'NEGATIVE'
    else:
        sentiment_type = 'NEUTRAL'
    return sentiment_type


#functions for textblob
def get_subjectivity(twt):
    return TextBlob(twt).sentiment.subjectivity

def get_polarity(twt):
    return TextBlob(twt).sentiment.polarity

def get_sentiment(score, positive_boundary, negative_boundary):
    if score > positive_boundary:
        sentiment_type = 'POSITIVE'
    elif score < negative_boundary:
        sentiment_type = 'NEGATIVE'
    else:
        sentiment_type = 'NEUTRAL'
    return sentiment_type