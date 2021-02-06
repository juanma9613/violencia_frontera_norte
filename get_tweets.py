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


def get_tweets_mentions(user,
                        api,
                        number_of_api_calls=4,
                        newest_id_possible_path='newest_id_mentions.json'):
    """get all mentions of a user, the query performed is the following to:<user> OR  @<user>

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
                                      count=100,
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
                                 count=100,
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

        if num_none_rows > 6:  #if 6 searches in a row are none then stop searching for that user
            print('exited because there were 6 consecutive calls giving none')
            break

    tweets_mentions = []
    retweets_mentions = []
    newest_tweet_id_mentions = {}

    for idx, tweet in enumerate(tweets):

        access_time = datetime.now().strftime("%Y %b %d %H:%M:%S")
        date = datetime.strptime(tweet._json["created_at"],
                                 '%a %b %d %H:%M:%S %z %Y')
        date_5 = date - timedelta(hours=5)
        date_format = date_5.strftime("%Y %b %d %H:%M:%S")

        if (tweet._json["user"]["screen_name"]
                == user) and (tweet._json["in_reply_to_screen_name"] == user):
            pass
        else:
            # getting tweets
            if ('RT @' not in tweet._json["full_text"]):
                temp = [
                    tweet._json["id"], access_time, date_format,
                    tweet._json["user"]["screen_name"], None, False, None,
                    tweet._json["full_text"]
                ]
                tweets_mentions.append(temp)

            # getting retweets
            else:
                temp = [
                    tweet._json["id"], access_time, date_format,
                    tweet._json["user"]["screen_name"], None, True,
                    tweet._json["retweeted_status"]["user"]["screen_name"],
                    tweet._json["retweeted_status"]["full_text"]
                ]
                retweets_mentions.append(temp)

    if (user not in newest_tweet_id_mentions) and (most_recent_id is not None):
        newest_tweet_id_mentions[user] = most_recent_id

    if most_recent_id is not None:
        with open(newest_id_possible_path, 'w') as outfile:
            json.dump(newest_tweet_id_mentions, outfile)

    return tweets_mentions, retweets_mentions


def get_tweets_timeline(user,
                        api,
                        number_of_api_calls=10,
                        newest_id_possible_path='newest_id_tweets.json'):
    """Function to get tweets and retweets from an user timeline

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
    n_queries_user = 0
    max_queries = number_of_api_calls

    tweets = tweet_batch = api.user_timeline(screen_name=user,
                                             tweet_mode="extended",
                                             count=100,
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

        tweet_batch = api.user_timeline(screen_name=user,
                                        result_type='recent',
                                        count=100,
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

        if num_none_rows > 6:  #if 6 searches in a row are none then stop searching for that user
            print('exited because there were 6 consecutive calls giving none')
            break

    tweets_lst = []
    retweets_lst = []
    newest_tweet_id = {}

    for idx, tweet in enumerate(tweets):

        access_time = datetime.now().strftime("%Y %b %d %H:%M:%S")
        date = datetime.strptime(tweet._json["created_at"],
                                 '%a %b %d %H:%M:%S %z %Y')
        date_5 = date - timedelta(hours=5)
        date_format = date_5.strftime("%Y %b %d %H:%M:%S")

        # getting tweets
        if ('RT @' not in tweet._json["full_text"]):
            temp = [
                tweet._json["id"], access_time, date_format,
                tweet._json["user"]["screen_name"], None, False, None,
                tweet._json["full_text"]
            ]
            tweets_lst.append(temp)

        # getting retweets
        else:
            temp = [
                tweet._json["id"], access_time, date_format,
                tweet._json["user"]["screen_name"], None, True,
                tweet._json["retweeted_status"]["user"]["screen_name"],
                tweet._json["retweeted_status"]["full_text"]
            ]
            retweets_lst.append(temp)

    if (user not in newest_tweet_id):
        newest_tweet_id[user] = most_recent_id

    if most_recent_id is not None:
        with open(newest_id_possible_path, 'w') as outfile:
            json.dump(newest_tweet_id, outfile)

    return tweets_lst, retweets_lst


if __name__ == "__main__":
    ## reading twitter api credentials.
    with open('creds.json') as json_file:
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

    df_tw_m = pd.DataFrame(tw_m,
                           columns=[
                               'id', 'fecha_consulta', 'fecha_escritura',
                               'cuenta_origen', 'query_busqueda', 'retweet',
                               'retweeted_from', 'texto'
                           ])

    df_rtw_m = pd.DataFrame(rtw_m,
                            columns=[
                                'id', 'fecha_consulta', 'fecha_escritura',
                                'cuenta_origen', 'query_busqueda', 'retweet',
                                'retweeted_from', 'texto'
                            ])
    # Getting tweets and retweets from user timeline
    # ----------------------------------------------
    tw, rtw = get_tweets_timeline(user, api=api)

    print(len(tw), "tweets", "and", len(rtw), "retweets collected")
    df_tw = pd.DataFrame(tw,
                         columns=[
                             'id', 'fecha_consulta', 'fecha_escritura',
                             'cuenta_origen', 'query_busqueda', 'retweet',
                             'retweeted_from', 'texto'
                         ])

    df_rtw = pd.DataFrame(rtw,
                          columns=[
                              'id', 'fecha_consulta', 'fecha_escritura',
                              'cuenta_origen', 'query_busqueda', 'retweet',
                              'retweeted_from', 'texto'
                          ])
