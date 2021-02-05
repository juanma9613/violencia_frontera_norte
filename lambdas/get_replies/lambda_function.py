import json
import tweepy
import xlrd
import os
import boto3
import datetime as dt


s3 = boto3.resource("s3")

def lambda_handler(event, context):
    with open('creds.json') as json_file:
        creds = json.load(json_file)

    auth = tweepy.AppAuthHandler(creds["client_key"], creds["client_secret"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    filepath= "users.xlsx"
    wb = xlrd.open_workbook(filepath)
    ws = wb.sheet_by_index(0)
    mylist = ws.col_values(0)[1:]
    users_order = [i.replace("@","") for i in mylist]

    sesion_tweets = {}
    max_queries = 430#200300 #440
    min_calls_per_user = 50 #50
    max_queries_per_user = 120 #150
    n_tweets_user = max_queries_per_user * 100
    n_queries = 0 
    lang = 'es'
    max_id = None

    project_bucket = parent_tweets_object = s3.Bucket('factored-internal-projects')
    content_object = s3.Bucket('factored-internal-projects').Object('efcv/replies_meta.json')
    users_meta = json.loads(content_object.get()['Body'].read().decode('utf-8'))
    if 'next_user' in users_meta:
        start_index = users_meta['next_user']
    else:
        start_index = 0
    users_order = users_order[start_index:] + users_order[:start_index]

    for user in users_order:
        print('user', user)
        if user in users_meta:
            if 'most_recent_id' in users_meta[user]:
                since_id = users_meta[user]['most_recent_id']
            else:
                since_id = None
        else:
            since_id = None

        n_queries_user = 0
        tweets = tweet_batch = api.search(q=f'to:{user}', count=n_tweets_user, lang = lang,
                                        since_id=since_id, max_id = max_id,
                                        tweet_mode="extended")  
        n_queries_user +=1 
        n_queries += 1

        most_recent_id = tweet_batch.since_id # the id of the most recent tweet obtained for this user

        num_none_rows = 0
        tweet_max_id = None
        
        while ((n_queries_user < max_queries_per_user) and (n_queries < max_queries)):
            if tweet_batch.max_id is not None:
                tweet_max_id = tweet_batch.max_id
            
            tweet_batch = api.search(q=f'to:{user}', lang = lang,
                                    count=n_tweets_user,
                                    max_id=tweet_max_id, 
                                    tweet_mode='extended',
                                    since_id=since_id)
            n_queries_user += 1
            n_queries += 1
            tweets.extend(tweet_batch)
            if len(tweet_batch) == 0:
                num_none_rows += 1
            else:
                num_none_rows = 0

            # end while if max_id is lower than since_id 
            if (tweet_max_id is not None) and (since_id is not None):
                if tweet_max_id < since_id:
                    print('exited because tweet_max_id < since_id')
                    break

            if num_none_rows > 6: #if 6 searches in a row are none then stop searching for that user
                print('exited because there were 6 consecutive calls giving none')
                break
                
        result_batch = [tweet._json for tweet in tweets]
        # save result batch and save most_recent_id for the user
        if user not in users_meta:
            users_meta[user] = dict()

        timestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        s3.Bucket('factored-internal-projects').put_object(Key='efcv/'+ user + '/' + timestamp  +'.json', 
                                                            Body=(bytes(json.dumps(result_batch).encode('UTF-8'))))
        if most_recent_id is not None:
            users_meta[user]['most_recent_id'] = most_recent_id

        users_meta["next_user"] = (users_order.index(user) + 1) % len(users_order)
        s3.Bucket('factored-internal-projects').put_object(Key='efcv/replies_meta.json', 
                                                            Body=(bytes(json.dumps(users_meta,
                                                                                indent=4).encode('UTF-8'))))

        if n_queries >= max_queries - min_calls_per_user: # if you dont have more calls
            break
            

    users_meta["next_user"] = (users_order.index(user) + 1) % len(users_order)

    s3.Bucket('factored-internal-projects').put_object(Key='efcv/replies_meta.json', 
                                                            Body=(bytes(json.dumps(users_meta,
                                                                                indent=4).encode('UTF-8'))))
    
    return {
        'statusCode': 200,
        'body': "succes"
    }

