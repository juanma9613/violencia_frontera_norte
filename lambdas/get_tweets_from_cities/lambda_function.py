import json
import boto3
import os
import tweepy
import datetime as dt 
from tweepy_data_request import *
print("starting script")
s3 = boto3.resource("s3")
consumer_key = ###INGRESAR CREDENCIALES
consumer_secret = ###INGRESAR CREDENCIALES
access_token = ###INGRESAR CREDENCIALES
access_token_secret = ###INGRESAR CREDENCIALES
df_ciudades = dict()
df_ciudades['Ciudad'] = ["Bogota",
                        "Tunja",
                        "Medellin y alrededores",
                        "Cali y Valle",
                        "Eje Cafetero",
                        "Bucaramanga",
                        "Barranquilla",
                        "Cartagena",
                        "Santa Marta",
                        "Valledupar"]

df_ciudades['Latitud']  =   [4.6952393,
                            5.4655073,
                            6.2686778,
                            3.3950646,
                            4.3599057,
                            6.8044658,
                            10.9838114,
                            10.3441754,
                            11.1798451,
                            10.4572102]
df_ciudades['Longitud']  =   [-74.1021148,
                            -73.3910258,
                            -75.5963918,
                            -76.5256639,
                            -75.6659209,
                            -72.7722553,
                            -74.8180178,
                            -75.5076564,
                            -74.1861947,
                            -73.2547337]
df_ciudades['Radio'] = ["14mi",
                    "45mi",
                    "17mi",
                    "40mi",
                    "40mi",
                    "32mi",
                    "16mi",
                    "11mi",
                    "13mi",
                    "5mi"]

def lambda_handler(event, context):
    print('event', event)
    print('context', context, dir(context))
    # old way
    #auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_token, access_token_secret)
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

    #auth = tweepy.AppAuthHandler(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    
    # change city index according to the city you want to get data from
    nTweets = 44000
    print('ntweets', nTweets)
    idioma ="es"
    n_ciudades = len(df_ciudades['Ciudad'])
    content_object = s3.Bucket('factored-internal-projects').Object('efcv/metadata.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    metadata = json.loads(file_content)
    content_object = s3.Bucket('factored-internal-projects').Object('efcv/meta_data_city.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    metadata_city = json.loads(file_content)
    print(metadata)

    if metadata.keys():
        city_index = metadata_city['city_index']
        str_city_index = str((int(city_index) + 1) % n_ciudades)
        city_index = int(str_city_index)
        if str_city_index in metadata.keys():
            if "most_recent_id" in metadata[str_city_index]:
                since_id = metadata[str_city_index]["most_recent_id"]
            else:
                since_id = None
        else:
            
            since_id = None
    else:
        city_index = 0
        str_city_index = str(city_index)
        since_id = None

    print('the since_id for the search is ', since_id)

    resultados_tweets, oldest_id, recent_id =  get_tweets_from_city(nTweets=nTweets, idioma=idioma,
                                    df_coordenadas_ciudades=df_ciudades,city_index=city_index,
                                                                    api=api,since_id=since_id)
    result_batch = [tweet._json for tweet in resultados_tweets]
    timestamp = dt.datetime.utcnow().strftime("%d%m%Y%H%M%S")
    print('timestamp',timestamp)
    s3.Bucket('factored-internal-projects').put_object(Key='efcv/'+ str_city_index + '/' + timestamp + "_" + str_city_index +'.json', 
                                                        Body=(bytes(json.dumps(result_batch).encode('UTF-8'))))
    # update recent_id in the metadata
    content_object = s3.Bucket('factored-internal-projects').Object('efcv/metadata.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')

    metadata = json.loads(file_content)
    
    
    if str_city_index in metadata:
        metadata[str_city_index]["most_recent_id"] = recent_id
    else:
        metadata[str_city_index] = {"most_recent_id":recent_id}
        
    

    s3object = s3.Object('factored-internal-projects', 'efcv/metadata.json')
    s3object.put(Body=(bytes(json.dumps(metadata).encode('UTF-8'))))


    metadata_city["city_index"] = str_city_index

    s3object = s3.Object('factored-internal-projects', 'efcv/meta_data_city.json')
    s3object.put(Body=(bytes(json.dumps(metadata_city).encode('UTF-8'))))


    
    return {
        'statusCode': 200,
        'body': "succes"
    }
