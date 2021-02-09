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
import argparse


def get_tweets_queries(query,
                       api,
                       geolocation_code=None,
                       exact_match=True,
                       separate_words_with_or=False,
                       exclude_words=[],
                       lang="es",
                       number_of_api_calls=10,
                       newest_id_possible_path='newest_id_query.json',
                       items_per_call=100):
  """runs a query with the search item and rules provided

    Args:
        query (str): search item
        api (tweepy.api.API): tweepy connection
        geolocation_code (str, optional): e.g. "-2.19616,-79.88621,14mi" -> Guayaquil [description]. Defaults to None.
        exact_match (bool, optional): [description]. Defaults to True.
        separate_words_with_or (bool, optional): [description]. Defaults to False. If exact_match is True, this value is ignored
        exclude_words (list, optional): [description]. Defaults to [].
        lang (str, optional): [description]. Defaults to "es".
        number_of_api_calls (int, optional): [description]. Defaults to 10.
        newest_id_possible_path (str, optional): registry file. Defaults to 'newest_id_query.json'.
        items_per_call (int, optional): [description]. Defaults to 100.

    Returns:
        df_tw_q, df_rtw_q (pd.DataFrame): dataframes with tweets and retweets based on queries
    """

  # dict to pass geocode and lang only if they are not None.
  kwargs_search = {}

  if lang is not None:
    kwargs_search["lang"] = lang

  if geolocation_code is not None:
    kwargs_search["geocode"] = geolocation_code

  if exact_match:
    query = f'"{query}"'

  elif separate_words_with_or:
    split_query = query.split()
    if len(split_query) > 1:
      query = " OR ".join(split_query)

  if len(exclude_words) > 0:
    format_exclude = " -" + " -".join(exclude_words)
  else:
    format_exclude = ""

  final_format_query = query + format_exclude

  if os.path.exists(newest_id_possible_path):
    with open(newest_id_possible_path) as json_file:
      newest_tweet_id_mentions = json.load(json_file)
      if final_format_query in newest_tweet_id_mentions:
        _since_id = newest_tweet_id_mentions[final_format_query]
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
                                    count=items_per_call,
                                    result_type='recent',
                                    since_id=_since_id,
                                    max_id=_max_id,
                                    **kwargs_search)

  n_queries += 1
  most_recent_id = tweet_batch.since_id
  num_none_rows = 0
  tweet_max_id = None

  while (n_queries < max_queries):
    if tweet_batch.max_id is not None:
      tweet_max_id = tweet_batch.max_id

    tweet_batch = api.search(q=final_format_query,
                             tweet_mode="extended",
                             count=items_per_call,
                             result_type='recent',
                             since_id=_since_id,
                             max_id=tweet_max_id,
                             **kwargs_search)
    n_queries += 1
    tweets.extend(tweet_batch)

    if len(tweet_batch) == 0:
      num_none_rows += 1
    else:
      num_none_rows = 0

    # End while if max_id is lower than since_id
    if (tweet_max_id is not None) and (_since_id is not None):
      if tweet_max_id < _since_id:
        print('exited because there are not more tweets to collect')
        break

    if num_none_rows > 5:  # If 5 searches in a row are none then stop searching for that user
      print('exited because there are not more tweets to collect')
      break

  df_tw_q, df_rtw_q = format_tweets(tweets, "null", "queries",
                                    final_format_query, geolocation_code)

  if most_recent_id is not None:
    newest_tweet_id_mentions[final_format_query] = most_recent_id

    with open(newest_id_possible_path, 'w') as outfile:
      json.dump(newest_tweet_id_mentions, outfile)

  return df_tw_q, df_rtw_q


def load_queries_info(path):

  with open(path) as json_file:
    queries_json = json.load(json_file)
  queries = queries_json["queries"]
  assert isinstance(queries, list), "queries must be a list"
  assert len(queries) > 0, "you must provide at least one query"
  geocode = queries_json["geocode"]
  exact_match = queries_json["exact_match"]
  separate_words_with_or = queries_json["separate_words_with_or"]
  exclude_words = queries_json["exclude_words"]
  language = queries_json["language"]
  return (queries, geocode, exact_match, language, separate_words_with_or,
          exclude_words)


def create_arg_parser():
  # Creates and returns the ArgumentParser object

  parser = argparse.ArgumentParser(
      description='script to collect tweets related with users')
  parser.add_argument("-Q",
                      "--queries_path",
                      default="queries.json",
                      type=str,
                      help="path to a json file containing the list of "
                      "queries and the other expressions to use in the search")
  return parser


if __name__ == "__main__":
  MAX_CALLS_TIMELINE_API = 900
  MAX_CALLS_QUERY_API = 180
  number_of_api_calls_per_query = 10
  # Reading twitter api credentials.
  with open('creds.json') as json_file:
    creds = json.load(json_file)

  auth = tweepy.AppAuthHandler(creds["client_key"], creds["client_secret"])
  api = tweepy.API(auth,
                   wait_on_rate_limit=True,
                   wait_on_rate_limit_notify=True)

  # Reading console args
  parser = create_arg_parser()
  args = parser.parse_args()

  queries_path = args.queries_path
  assert os.path.exists(
      queries_path), "you didn't add a path to read queries from"

  (queries, geolocation, exact_match, language, separate_words_with_or,
   exclude_words) = load_queries_info(queries_path)

  assert len(
      queries) > 0, "you need to provide a json file with at least one query"

  assert len(
      queries) < MAX_CALLS_QUERY_API, "please reduce the number of queries"

  if number_of_api_calls_per_query * len(queries) > MAX_CALLS_QUERY_API:
    number_of_api_calls_per_query = MAX_CALLS_QUERY_API // len(queries)

  dfs = []
  for i in queries:
    tw, rts = get_tweets_queries(
        query=i,
        api=api,
        geolocation_code=geolocation,
        exact_match=exact_match,
        separate_words_with_or=separate_words_with_or,
        exclude_words=exclude_words,
        lang=language,
        number_of_api_calls=number_of_api_calls_per_query,
        items_per_call=100)

    print(len(tw), "tweets", "and", len(rts), "retweets collected for query: ",
          i)

    dfs.append(tw)
    dfs.append(rts)

  access_time = datetime.now().strftime("%Y_%b_%d_%H:%M:%S")
  out = access_time + "_queries.csv"
  all_dfs = pd.concat(dfs, ignore_index=True)
  all_dfs.to_csv(out, sep='\t', index=False, header=True, quoting=csv.QUOTE_ALL)
