
import snscrape.modules.twitter as sntwitter
import pandas as pd
import datetime
import pytz
from function import read_csv_to_list, write_header, append_content
import configparser

config = configparser.ConfigParser()
config.read('configuration.ini')

since_date = config['data_extraction']['since_date']
until_date = config['data_extraction']['until_date']
max_num_per_call = int(config['data_extraction']['max_num_per_call'])
users_file = config['data_extraction']['users_file']
keywords_file = config['data_extraction']['keywords_file']
time_zone = config['data_extraction']['time_zone']
tweets_file_path = config['data_extraction']['tweets_file_path']
counts_file_path = config['data_extraction']['counts_file_path']
language = config['data_extraction']['language']


current_time = datetime.datetime.now(pytz.timezone(time_zone))
current_time_string = current_time.strftime("%Y%m%d-%H%M%S")

tweets_file = tweets_file_path + '/tweets_' + current_time_string + '.csv'
counts_file = counts_file_path + '/counts_' + current_time_string + '.csv'

# print('Input users file: ' + users_file)
# print('Input keywords file' + keywords_file)
# print('Output tweets file: ' + tweets_file)
# print('Output counts file: ' + counts_file)

# from_users = read_csv_to_list(users_file)
keywords = read_csv_to_list(keywords_file)

# print(from_users)
# print(keywords)
total_count = 0

# write_header(tweets_file, ['Datetime', 'Tweet Id', 'Text', 'Username'])
# write_header(counts_file, ['Username', 'Keyword', 'Count'])

tweets_col = ['search_term', 'datetime', 'tweet_id', 'text', 'username', 'reply_count', 'retweet_count', 'like_count', 'quote_count']

#max_num_per_call = 100

entity_name = None
for keyword in keywords:
    if keyword == "<NEXT>":
        entity_name = None
        continue

    if not entity_name:
        entity_name = keyword
        tweets_file = tweets_file_path + '/tweets_' + entity_name + '.csv'
        write_header(tweets_file, tweets_col)

    count = 0
    tweets_list = []
    counts_list = []

    #result = sntwitter.TwitterSearchScraper(keyword + ' from:' + from_user + ' since:' + since_date + ' until:' + until_date + ' lang:' + language).get_items()
    #result = sntwitter.TwitterSearchScraper(keyword + ' since:' + since_date + ' until:' + until_date + ' lang:' + language + ' min_replies:5').get_items()
    result = sntwitter.TwitterSearchScraper(keyword + ' since:' + since_date + ' until:' + until_date + ' lang:' + language).get_items()
    for i, tweet in enumerate(result): 
        if (i%1000 == 0):
            print(entity_name + ' : ' + keyword + ' : ' + str(i))   
        if (i > max_num_per_call or tweet is None):
            break
        tweets_list.append([keyword, tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount, tweet.likeCount, tweet.quoteCount])
        count += 1
    counts_list.append([entity_name, keyword, count])
    total_count += count
    
    tweets_df = pd.DataFrame(tweets_list, columns=tweets_col)
    counts_df = pd.DataFrame(counts_list, columns=['Username', 'Keyword', 'Count'])

    

    tweets_df.to_csv(tweets_file, encoding='utf-8', mode='a', index=False, header=None)
    counts_df.to_csv(counts_file, encoding='utf-8', mode='a', index=False, header=None)



# from_users = ['user']
# for from_user in from_users:
#     for keyword in keywords:
#         entity_name = keyword
#         if keyword == "<NEXT>":
#             print("sdf")
#         count = 0
#         tweets_list = []
#         counts_list = []

#         #result = sntwitter.TwitterSearchScraper(keyword + ' from:' + from_user + ' since:' + since_date + ' until:' + until_date + ' lang:' + language).get_items()
#         result = sntwitter.TwitterSearchScraper(keyword + ' since:' + since_date + ' until:' + until_date + ' lang:' + language).get_items()
#         for i, tweet in enumerate(result): 
#             if (i%100 == 0):
#                 print(from_user + ' : ' + keyword + ' : ' + str(i))   
#             if (i > max_num_per_call or tweet is None):
#                 break
#             tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount, tweet.likeCount, tweet.quoteCount])
#             count += 1
#         counts_list.append([from_user, keyword, count])
#         total_count += count
        
#         tweets_df = pd.DataFrame(tweets_list, columns=['datetime', 'tweet_id', 'text', 'username', 'reply_count', 'retweet_count', 'like_count', 'quote_count'])
#         counts_df = pd.DataFrame(counts_list, columns=['Username', 'Keyword', 'Count'])

#         tweets_file = tweets_file_path + '/tweets_' + keyword + '.csv'

#         tweets_df.to_csv(tweets_file, encoding='utf-8', mode='a', index=False, header=True)
#         counts_df.to_csv(counts_file, encoding='utf-8', mode='a', index=False, header=None)

# append_content(counts_file, ['Total Count', str(total_count)])
