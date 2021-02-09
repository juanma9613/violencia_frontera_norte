# violencia_frontera_norte
Repository to get tweets from twitter API related with user accounts or with specific queries, we have two scripts to get data, below we have a brief description of each.

- [get_tweets.py](./get_tweets.py) : gets tweets from accounts specified in a list.
- [get_tweets_queries.py](./get_tweets_queries.py) : gets tweets based on a list of queries.

**Note** : to run any of the scripts you should have in this folder a file called creds.json with your twitter API credentials.
## get_tweets.py

This script should be run providing the path to a txt file containing a user id in each row.

````
python get_tweets --users_path users.txt
````

it usually performs 10 api calls per user in the list, each call will retrieve 100 or 200 tweets with mentions of the user or tweets/retweets from the timeline

## get_tweets_queries.py

This script should be run providing the path to a json file containing the queries and some additional information, there are two examples in the repo, one is [queries.json](./queries.json), which doesn't include words to exclude, and the other is [queries_geocode.json](./queries_geocode.json), when the geocode provided is not null and we want to exclude the word "politica" from the search.

````
python get_tweets_queries --queries_path queries.json
````


the fields in the json are the following.

- queries: A list of strings where each string can have one or more words or expresions.
- exclude_words: a list with words that we want to exclude from all the queries.
- A geolocation code: a string representing latitude, longitude and radious to retrieve tweets from, or null to not limit the query by location
- exact_match: true or false, wheter to search exactly each sentence/word in the list of queries above.
- separate_words_with_or : true or false, ignored when exact match is true. Each string in queries is an query that can have one 
or more words, in case this variable is set to true, we will separate each word with the or operator from twitter. let's say that queries[i] == "violencia ecuador"
if separate_words_with_or == True and exact_match=False, the query in this case is violencia OR ecuador.
