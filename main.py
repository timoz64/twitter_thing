import tweepy
import pandas as pd
import os
#import psycopg2
from sqlalchemy import create_engine
from datetime import timedelta

conn_string = os.environ["IMAC_POSTGRESS_CONN_STRING"]
db = create_engine(conn_string)
conn = db.connect()

client = tweepy.Client(bearer_token=os.environ["TWITTER_BEARER_TOKEN"])


def get_tweet_count_for_topic(topic, starttime):
    query = f"{topic} -is:retweet"
    try:
        count_tweets = client.get_recent_tweets_count(query, start_time=starttime)
        df = pd.DataFrame()
        for count in count_tweets.data:
            df_dictionary = pd.DataFrame([count])
            df = pd.concat([df, df_dictionary], ignore_index=True)
            df['currency'] = topic
        return df
    except Exception as e:
        return e

def get_latest_starttime_from_postgress(currency):
    q = f"""SELECT MAX(start_date) + interval '1 hour' as latest
    FROM public.twitter_tweet_count
    where currency = '{currency}';"""
    latest = pd.read_sql_query(sql=q, con=conn, index_col=None)['latest'].iat[0]
    if isinstance(latest, pd.Timestamp):
        return latest
    else:
        return pd.Timestamp.now() - timedelta(days=5)


def do_everything(currency, starttime):

    try:
        all_topics = get_tweet_count_for_topic(currency, starttime)
        if isinstance(all_topics, pd.DataFrame):
            all_topics.rename(columns={"end": "end_date", "start": "start_date"}, inplace=True)
            all_topics["end_date"] = pd.to_datetime(all_topics["end_date"])
            all_topics["start_date"] = pd.to_datetime(all_topics["start_date"])
            all_topics.head()

            count = all_topics.to_sql('twitter_tweet_count', con=conn, if_exists='append', index=False)
            print(f'Loading {count} lines of {currency} was success!')
        else:
            print(f'{currency} FAIL!')
    except Exception as e:
        print(e)

if __name__ == "__main__":
    currencies = ['bitcoin', 'cardano', 'ethereum']
    for currency in currencies:
        starttime = get_latest_starttime_from_postgress(currency)
        do_everything(currency, starttime)
