import json
import os
import boto3
import math
from datetime import date
from datetime import datetime
from twarc import Twarc
import datetime as dt
import xlrd


s3 = boto3.resource("s3")

def lambda_handler(event, context):
    today = datetime.now()
    with open('creds.json') as json_file:
        creds = json.load(json_file)

    twarc_object = Twarc(creds["client_key"], 
                         creds["client_secret"], 
                         creds["access_token"], 
                         creds["access_token_secret"])

    filepath= "users.xlsx"

    wb = xlrd.open_workbook(filepath)
    ws = wb.sheet_by_index(0)
    mylist = ws.col_values(0)[1:]
    users = [i.replace("@","") for i in mylist]

    project_bucket = parent_tweets_object = s3.Bucket('factored-internal-projects')
    content_object = s3.Bucket('factored-internal-projects').Object('efcv/parent_tweets.json')
    parent_tweets_ids = json.loads(content_object.get()['Body'].read().decode('utf-8'))

    if 'since_id' in parent_tweets_ids:
        since_id = parent_tweets_ids['since_id']
    else:
        since_id = None

    tweet_counter = 0
    min_id = math.inf
    max_id = - math.inf
    tw_ids = dict()

    for user in users:
        tw_ids[user] = {}
        for tweet in twarc_object.timeline(screen_name=user,
                                           since_id = since_id,
                                           max_pages=2):
            current_id = tweet["id"]

            if current_id > max_id:
                max_id = current_id
            if current_id < min_id:
                min_id = current_id

            if ('RT @' not in tweet["full_text"]): 
                tw_ids[user][tweet["id_str"]] = tweet
    
    print('the download of the tweets ended saving to s3', min_id, max_id)
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    s3.Bucket('factored-internal-projects').put_object(Key='efcv/user_original_tweets'+  '/' + timestamp + '.json', 
                                                        Body=(bytes(json.dumps(tw_ids, indent=4).encode('UTF-8'))))

    s3object = s3.Object('factored-internal-projects', 'efcv/parent_tweets.json')

    
    if max_id != -math.inf:
        parent_tweets_ids['since_id'] = max_id
    if 'oldest_id' not in parent_tweets_ids and min_id != math.inf:
        parent_tweets_ids['oldest_id'] = min_id
    

    s3object.put(Body=(bytes(json.dumps(parent_tweets_ids,indent=4, sort_keys=True).encode('UTF-8'))))

    print('parent dict saved', parent_tweets_ids)

    return {
        'statusCode': 200,
        'body': "succes"
    }

