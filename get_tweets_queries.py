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
from get_tweets import format_tweets

def get_tweets_queries(query,
                       api,
                       geolocation_code=None,
                       exact_match=True,
                       separate_words_with_or=False,
                       exclude_words = [],
                       lang="es",
                       number_of_api_calls=10,
                       newest_id_possible_path='newest_id_query.json',
                       items_per_call=100):
    """[summary]

    Args:
        query (str): search item
        api (tweepy.api.API): tweepy connection
        geolocation_code (str, optional): e.g. "-2.19616,-79.88621,14mi" -> Guayaquil [description]. Defaults to None.
        exact_match (bool, optional): [description]. Defaults to True.
        separate_words_with_or (bool, optional): [description]. Defaults to False.
        exclude_words (list, optional): [description]. Defaults to [].
        lang (str, optional): [description]. Defaults to "es".
        number_of_api_calls (int, optional): [description]. Defaults to 10.
        newest_id_possible_path (str, optional): registry file. Defaults to 'newest_id_query.json'.
        items_per_call (int, optional): [description]. Defaults to 100.

    Returns:
        df_tw_q, df_rtw_q (pd.DataFrame): dataframes with tweets and retweets based on queries
    """

    if exact_match:
        format_query = f'"{query}"'

    elif separate_words_with_or:
        if len(query.split()) > 1:            
            lst_q = query.split()
            join_query = " OR ".join(lst_q)
            format_query = f'{join_query}'
        else:
            format_query = f'{query}'
    else:
        format_query = f'{query}'

    if len(exclude_words) > 0:  
            join_exclude = " -" + " -".join(exclude_words)
            format_exclude = f'{join_exclude}'
    else:
        format_exclude = ""
    
    final_format_query = format_query + format_exclude

    if os.path.exists(newest_id_possible_path):
        with open(newest_id_possible_path) as json_file:
            newest_tweet_id_mentions = json.load(json_file)
            if query in newest_tweet_id_mentions:
                _since_id = newest_tweet_id_mentions[query]
            else:
                _since_id = None
    else:
        newest_tweet_id_mentions = {}
        _since_id = None

    _max_id = None
    n_queries = 0
    max_queries = number_of_api_calls

    tweets = tweet_batch = api.search(q=final_format_query,
                                      tweet_mode="extended",
                                      geolocation_code=geolocation_code,
                                      lang=lang,
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

        tweet_batch = api.search(q=final_format_query,
                                 tweet_mode="extended",
                                 geolocation_code=geolocation_code,
                                 lang=lang,
                                 count=items_per_call,
                                 result_type='recent',
                                 since_id=_since_id,
                                 max_id=tweet_max_id)
        n_queries += 1
        tweets.extend(tweet_batch)

        if len(tweet_batch) == 0:
            num_none_rows += 1
        else:
            num_none_rows = 0

        # End while if max_id is lower than since_id
        if (tweet_max_id is not None) and (_since_id is not None):
            if tweet_max_id < _since_id:
                print('exited because tweet_max_id < since_id')
                break

        if num_none_rows > 5:  # If 5 searches in a row are none then stop searching for that user
            print('exited because there were 6 consecutive calls giving none')
            break

    df_tw_q, df_rtw_q = format_tweets(tweets, "null", "queries", final_format_query, geolocation_code)

    if most_recent_id is not None:
        newest_tweet_id_mentions[query] = most_recent_id

    #if most_recent_id is not None:
        with open(newest_id_possible_path, 'w') as outfile:
            json.dump(newest_tweet_id_mentions, outfile)

    return df_tw_q, df_rtw_q


if __name__ == "__main__":
    
    # Reading twitter api credentials.
    with open('creds.json') as json_file:
        creds = json.load(json_file)

    auth = tweepy.AppAuthHandler(creds["client_key"], creds["client_secret"])
    api = tweepy.API(auth,
                     wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    # Parameters
    # ---------------
    _querys = ["violencia", "paz" , "frontera", "mamá me mima", "ecuador"]
    geolocation = "-2.19616,-79.88621,14mi"
    language = "es"
    exact_match = True
    separate_words_with_or = False
    exclude_words = ["la"]

    '''
    If exact_match == False and separate_words_with_or == False:

        e.g. query = watching now	(containing both “watching” and “now”. This is the default operator.)

    If exact_match == True:

        e.g. query = “happy hour”	(containing the exact phrase “happy hour”.)

    If separate_words_with_or == True and exact_match == False:

        e.g. query = love OR hate	(containing either “love” or “hate” (or both).)

    If exclude_words == ["root", "tor"]:

        e.g. query = query[i] -root -tor (containing “query[i]” but not “root” and "tor".)
    '''
    
    dfs = []
    for i in _querys:
        tw, rts = get_tweets_queries(query=i,
                                    api=api,
                                    geolocation_code=geolocation,
                                    exact_match=exact_match,
                                    separate_words_with_or = separate_words_with_or,
                                    exclude_words = exclude_words,
                                    lang=language,
                                    number_of_api_calls=10,
                                    items_per_call=100)

        print(len(tw), "tweets", "and", len(rts), "retweets collected for query: ", i)

        dfs.append(tw)
        dfs.append(rts)
    
    access_time = datetime.now().strftime("%Y_%b_%d_%H:%M:%S")
    out = access_time + "_queries.csv"
    all_dfs = pd.concat(dfs, ignore_index=True)
    all_dfs.to_csv(out,
                sep='\t',
                index=False,
                header=True,
                quoting=csv.QUOTE_ALL)
