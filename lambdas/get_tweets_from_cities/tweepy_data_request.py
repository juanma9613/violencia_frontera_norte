import tweepy
import json

def get_n_tweets(query, n, geocode, lang, api, since_id=None, max_id=None):
    """
    This function tries to get up to 'n' tweets with the query given.
    
    The motivation to create this function is that the api.search function isn't able to 
    get more than 100 tweets with a unique call, and the cursor was presenting problems
    -------
    INPUTS:
    query: the query to retrieve tweets
    n: the expected number of tweets to get
    geocode: the geocode to get tweets.
    lang: the language to retrieve tweets.
    since_id: The id of the oldest possible tweet to get.
    max_id: the id of the newest tweet to get
    
    OUTPUTS:
    tweets: The tweets obtained from the api call
    max_id: the id of the oldest tweet obtained.
    new_since_id: the id of the most recent tweet obtained
    """
    
    #sometimes the queries don't return 100 results in a call even though there are a lot of items to get
    # so it's better to call the function for 50% more iterations than expected
    _max_queries = min(int((n // 100) *  1.4), 439)
    
    # added tweet mode extended to receive full text only according to https://github.com/tweepy/tweepy/issues/935
    tweets = tweet_batch = api.search(q=query, count=n, geocode = geocode, lang = lang,
                                      since_id=since_id, max_id = max_id, tweet_mode="extended")  
    new_since_id = tweet_batch.since_id
    ids_set = set()
    print('initial_since_id', since_id)
    num_none_rows = 0
    
    tweet_max_id = None
    
    ct = 1
    while len(tweets) < n and ct < _max_queries:
        
        ids = [tw.id for tw in tweet_batch]
        intersect = ids_set.intersection(set(ids))
        ids_set = ids_set.union(set(ids))
        #print('inwhile', since_id, len(tweet_batch), tweet_batch.max_id)
        
        if tweet_batch.max_id is not None:
            tweet_max_id = tweet_batch.max_id
        
        tweet_batch = api.search(q=query, geocode = geocode, lang = lang,
                                 count=n - len(tweets),
                                 max_id=tweet_max_id, 
                                 tweet_mode='extended',
                                 since_id=since_id)
        tweets.extend(tweet_batch)
        ct += 1
        if len(intersect) > 0:
            print('The download for this city is duplicating tweets')
            
        if len(tweet_batch) == 0:
            num_none_rows += 1
        else:
            num_none_rows = 0
        # end while if max_id is lower than since_id 
        if (tweet_max_id is not None) and (since_id is not None):

            if tweet_max_id < since_id:
                print('exited because tweet_max_id < since_id')

                break
        if num_none_rows > 15: #if 10 searches in a row are none then stop searching
            print('exited because there were 15 consecutive calls giving none')
            break
            
    return tweets, tweet_max_id, new_since_id


def get_tweets_from_city(nTweets,idioma,df_coordenadas_ciudades,
                         city_index, api, max_id=None, since_id=None):
    """
    This function gets all the possible tweets for a query with the geolocation of a given city.
    If since_id and max_id provided, all the tweets will be obtained in that range
    """
    response_cities = []
    city = df_coordenadas_ciudades["Ciudad"][city_index]
    geocode = str(df_coordenadas_ciudades['Latitud'][city_index])+','+str(df_coordenadas_ciudades["Longitud"][city_index]) +','+ df_coordenadas_ciudades['Radio'][city_index]
    response_city, oldest_id, recent_id = get_n_tweets(query= "", n = nTweets, geocode=geocode, lang=idioma,
                                                       api=api, max_id=max_id, since_id=since_id)
    print('downloaded {0} tweets for the city {1}'.format(len(response_city), city_index))
    return response_city, oldest_id, recent_id





