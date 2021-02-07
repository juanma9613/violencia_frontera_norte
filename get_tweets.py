import tweepy
import pandas as pd
import json
import math
import datetime
import tweepy
import os
from datetime import datetime, timedelta
import re
import csv

def format_tweets(tweets, user, _type):

    tweets_lst = []
    retweets_lst = []  

    for idx, tweet in enumerate(tweets):

        access_time = datetime.now().strftime("%Y %b %d %H:%M:%S")
        date = datetime.strptime(tweet._json["created_at"],
                                 '%a %b %d %H:%M:%S %z %Y')
        date_5 = date - timedelta(hours=5)
        date_format = date_5.strftime("%Y %b %d %H:%M:%S")

        if _type == "mentions" and (tweet._json["user"]["screen_name"]
                == user) and (tweet._json["in_reply_to_screen_name"] == user):
            pass
            
        else:
            
            # getting tweets
            if ('RT @' not in tweet._json["full_text"]):
                temp = [
                    tweet._json["id"], access_time, date_format,
                    tweet._json["user"]["screen_name"], None, False, None, _type,
                    tweet._json["full_text"]
                ]
                tweets_lst.append(temp)

            # getting retweets
            else:
                temp = [
                    tweet._json["id"], access_time, date_format,
                    tweet._json["user"]["screen_name"], None, True,
                    tweet._json["retweeted_status"]["user"]["screen_name"], _type,
                    tweet._json["retweeted_status"]["full_text"]
                ]
                retweets_lst.append(temp)
        
    df_tw = pd.DataFrame(tweets_lst,
                         columns=[
                             'id', 'fecha_consulta', 'fecha_escritura',
                             'cuenta_origen', 'query_busqueda', 'retweet',
                             'retweeted_from', 'type', 'texto'
                         ])

    df_rtw = pd.DataFrame(retweets_lst,
                          columns=[
                              'id', 'fecha_consulta', 'fecha_escritura',
                              'cuenta_origen', 'query_busqueda', 'retweet',
                              'retweeted_from', 'type', 'texto'
                          ])
    
    return df_tw, df_rtw


def get_tweets_mentions(user,
                        api,
                        number_of_api_calls=10,
                        newest_id_possible_path='/home/noone/Desktop/vfn/violencia_frontera_norte/newest_id_mentions.json',
                        items_per_call=100):
    """get all mentions of a user, the query performed is the following to:<user> OR  @<user>

    Link
    - https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
    - https://docs.tweepy.org/en/latest/api.html?highlight=search#API.search
    NOTE:
    Requests / 15-min window (user auth) : 180
    Requests / 15-min window (app auth)	: 450
    Args:
        user ([type]): [description]
        api ([type]): [description]
        number_of_api_calls (int, optional): [description]. Defaults to 4.
        newest_id_possible_path (str, optional): [description]. Defaults to 'newest_id_mentions.json'.

    Returns:
        [type]: [description]
    """

    if os.path.exists(newest_id_possible_path):
        with open(newest_id_possible_path) as json_file:
            data = json.load(json_file)
            if user in data:
                _since_id = data[user]
            else:
                _since_id = None
    else:
        _since_id = None

    _max_id = None
    n_queries = 0
    max_queries = number_of_api_calls

    tweets = tweet_batch = api.search(q=f'to:{user} OR @{user}',
                                      tweet_mode="extended",
                                      count=items_per_call,
                                      result_type='recent',
                                      since_id=_since_id,
                                      max_id=_max_id)

    n_queries += 1
    most_recent_id = tweet_batch.since_id
    num_none_rows = 0
    tweet_max_id = None

    while (n_queries < max_queries):
        if tweet_batch.max_id is not None:
            tweet_max_id = tweet_batch.max_id

        tweet_batch = api.search(q=f'to:{user} OR @{user}',
                                 result_type='recent',
                                 count=items_per_call,
                                 max_id=tweet_max_id,
                                 tweet_mode='extended',
                                 since_id=_since_id)

        n_queries += 1
        tweets.extend(tweet_batch)

        if len(tweet_batch) == 0:
            num_none_rows += 1
        else:
            num_none_rows = 0

        # end while if max_id is lower than since_id
        if (tweet_max_id is not None) and (_since_id is not None):
            if tweet_max_id < _since_id:
                print('exited because tweet_max_id < since_id')
                break

        if num_none_rows > 5:  #if 5 searches in a row are none then stop searching for that user
            print('exited because there were 6 consecutive calls giving none')
            break

    newest_tweet_id_mentions = {}

    df_tw_m, df_rtw_m = format_tweets(tweets, user, "mentions")

    if (user not in newest_tweet_id_mentions) and (most_recent_id is not None):
        newest_tweet_id_mentions[user] = most_recent_id

    if most_recent_id is not None:
        with open(newest_id_possible_path, 'w') as outfile:
            json.dump(newest_tweet_id_mentions, outfile)

    return df_tw_m, df_rtw_m


def get_tweets_timeline(user,
                        api,
                        number_of_api_calls=10,
                        newest_id_possible_path='/home/noone/Desktop/vfn/violencia_frontera_norte/newest_id_tweets.json',
                        items_per_call=200):
    """Function to get tweets and retweets from an user timeline

    links:
    - https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/api-reference/get-statuses-user_timeline
    - https://docs.tweepy.org/en/latest/api.html?highlight=search#API.user_timeline

    NOTE: 
    the maximum number of calls in a 15-min window is 900
    Args:
        user (str): screen name of an user
        api ([type]): [description]
        number_of_api_calls (int, optional): [description]. Defaults to 10.
        newest_id_possible_path (str, optional): [description]. Defaults to 'newest_id_tweets.json'.

    Returns:
        [type]: [description]
    """

    if os.path.exists(newest_id_possible_path):
        with open(newest_id_possible_path) as json_file:
            data = json.load(json_file)
            if user in data:
                _since_id = data[user]
            else:
                _since_id = None
    else:
        _since_id = None

    _max_id = None
    n_queries = 0
    max_queries = number_of_api_calls

    tweets = tweet_batch = api.user_timeline(screen_name=user,
                                             tweet_mode="extended",
                                             result_type='recent',
                                             count=items_per_call,
                                             since_id=_since_id,
                                             max_id=_max_id)

    n_queries += 1
    most_recent_id = tweet_batch.since_id
    num_none_rows = 0
    tweet_max_id = None

    while (n_queries < max_queries):

        if tweet_batch.max_id is not None:
            tweet_max_id = tweet_batch.max_id

        tweet_batch = api.user_timeline(screen_name=user,
                                        tweet_mode='extended',
                                        result_type='recent',
                                        count=items_per_call,
                                        max_id=tweet_max_id,
                                        since_id=_since_id)

        n_queries += 1
        tweets.extend(tweet_batch)

        if len(tweet_batch) == 0:
            num_none_rows += 1
        else:
            num_none_rows = 0

        # end while if max_id is lower than since_id
        if (tweet_max_id is not None) and (_since_id is not None):
            if tweet_max_id < _since_id:
                print('exited because tweet_max_id < since_id')
                break

        if num_none_rows > 6:  #if 6 searches in a row are none then stop searching for that user
            print('exited because there were 6 consecutive calls giving none')
            break

    newest_tweet_id = {}

    df_tw, df_rtw = format_tweets(tweets, user, "timeline")
    
    if (user not in newest_tweet_id):
        newest_tweet_id[user] = most_recent_id

    if most_recent_id is not None:
        with open(newest_id_possible_path, 'w') as outfile:
            json.dump(newest_tweet_id, outfile)

    return df_tw, df_rtw


if __name__ == "__main__":
    ## reading twitter api credentials.
    with open('/home/noone/Desktop/vfn/violencia_frontera_norte/creds.json') as json_file:
        creds = json.load(json_file)

    user = "@DEFENSORIAEC"
    user = user.replace("@", "")

    auth = tweepy.AppAuthHandler(creds["client_key"], creds["client_secret"])
    api = tweepy.API(auth,
                     wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    # Getting mentions
    #  ---------------
    tw_m, rtw_m = get_tweets_mentions(user, api=api)
    print((len(tw_m) + len(rtw_m)), "mentions collected")

    # Getting tweets and retweets from user timeline
    # ----------------------------------------------
    tw, rtw = get_tweets_timeline(user, api=api)
    print(len(tw), "tweets", "and", len(rtw), "retweets collected")

    dfs = [tw_m, rtw_m, tw, rtw]
    all_dfs = pd.concat(dfs, ignore_index=True)

    access_time = datetime.now().strftime("%Y_%b_%d_%H:%M:%S")
    out = access_time + "_cuentas.csv"

    all_dfs.to_csv(out,
                   sep='\t',
                   index=False,
                   header=True,
                   quoting=csv.QUOTE_ALL)
