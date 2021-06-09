#Get Libiaries
from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytwitter import Api
from pytwitter.models.user import PublicMetrics
from requests_oauthlib import OAuth1Session
import requests
import os
import json
import pyodbc

#Pass in Twitter authentication information, and SQL information for it to store data
def daily(user_id,username,BEARER_TOKEN,consumer_key,consumer_secret,access_token,access_token_secret,Authserver,Authdatabase,Authusername,Authpassword,stable_name,App_rate_limit):

    today = datetime.now().date()

    #Updates tweets over the last year
    last_update = (datetime.now() - timedelta(days=1*365)).date()

    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+Authserver+';DATABASE='+Authdatabase+';UID='+Authusername+';PWD='+ Authpassword)
    cursor = cnxn.cursor()

    sql_query = pd.read_sql_query('SELECT * FROM '+stable_name,cnxn)

    cursor.close()

    def parse_id():
        n = json_response['meta']['result_count']
        for i in range(0,n):
            next_id = json_response['data'][i]['id']
            id_list.append(np.int64(next_id))

    # First pagination
    def auth():
        return BEARER_TOKEN

    def create_url():
        tweet_fields = "tweet.fields=author_id"
        start_time = "start_time={}T01:00:00Z".format(last_update)
        end_time = "end_time={}T01:00:00Z".format(today)
        url = "https://api.twitter.com/2/users/{}/tweets?{}&{}&{}&exclude=retweets".format(user_id, tweet_fields, start_time, end_time)
        return url

    def create_headers(bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def connect_to_endpoint(url, headers):
        response = requests.request("GET", url, headers=headers)
        #print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    id_list = []
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)
    parse_id()

    def create_url():
        tweet_fields = "tweet.fields=author_id"
        start_time = "start_time={}T01:00:00Z".format(last_update)
        print(start_time)
        end_time = "end_time={}T01:00:00Z".format(today)
        pagination = "pagination_token={}".format(pagination_token)
        url = "https://api.twitter.com/2/users/{}/tweets?{}&{}&{}&{}&".format(user_id, tweet_fields, start_time, end_time, pagination)
        return url

    # Pagination loop
    while True:
        try:
            print(json_response['meta']['next_token'])
            pagination_token = json_response['meta']['next_token']
            bearer_token = auth()
            url = create_url()
            headers = create_headers(bearer_token)
            json_response = connect_to_endpoint(url, headers)
            parse_id()
        except KeyError:
            break

    # Gets from twitter public information from the previous df
    def auth():
        return BEARER_TOKEN

    def create_url(val):
        tweet_fields = "tweet.fields=lang,author_id,created_at,entities,geo,id,public_metrics,source,text,referenced_tweets"
        ids = "ids=%s" % str(val)
        # You can adjust ids to include a single Tweets.
        # Or you can add to up to 100 comma-separated IDs
        url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
        return url

    def create_headers(bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def connect_to_endpoint(url, headers):
        response = requests.request("GET", url, headers=headers)
    #  print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    temp_df = pd.DataFrame(columns = ['text', 'created_at','id','source','author_id','retweet_count','reply_count','like_count','quote_count','lang','created_date','hour','URL'])

    n = len(id_list)
    rate_limit = 0
    for x in range(0,n):
        rate_limit=rate_limit+1
        App_rate_limit = App_rate_limit+1
        if(rate_limit > 850 or App_rate_limit > 850):
            print("Rate limit hit. Waiting until cooldown period is over.")
            sleep(1000) # 15 minutes
            rate_limit=0
            App_rate_limit=0
            print("Resuming")
        bearer_token = auth()
        url = create_url(id_list[x])
        headers = create_headers(bearer_token)
        json_response = connect_to_endpoint(url, headers)
        id_values = []
        retweets = []
        replies = []
        likes = []
        quotes = []
        for i in range(0,len(json_response['data'])):
            id_values.append(json_response['data'][i]['id'])
            retweets.append(json_response['data'][i]['public_metrics']['retweet_count'])
            replies.append(json_response['data'][i]['public_metrics']['reply_count'])
            likes.append(json_response['data'][i]['public_metrics']['like_count'])
            quotes.append(json_response['data'][i]['public_metrics']['quote_count'])

        df = pd.DataFrame.from_dict(json_response['data'])
        df = df.drop(columns='public_metrics')
        df.insert(5, "retweet_count", retweets, True)
        df.insert(6, "reply_count", replies, True)
        df.insert(7, "like_count", likes, True)
        df.insert(8, "quote_count", quotes, True)
        df.insert(9, "string ID", id_values, True)
        df['timestamp'] = pd.to_datetime(df['created_at'])
        df['created_date'] = df['timestamp'].apply(lambda x:str(x.date()))
        df['hour'] = df['timestamp'].apply(lambda x:str(x.hour))
        df = df.drop(columns='timestamp')
        df['URL'] = 'https://twitter.com/' + username + '/status/' + df['string ID']
        df = df.drop(columns='string ID')
        temp_df = temp_df.append(df, ignore_index = True)

    print(temp_df.head(2))

    print(json_response)

    #Get list of tweets in the last 30 days. Private data can only be pulled for the last 30 days.
    df_private = pd.DataFrame()
    df_private['id'] = temp_df['id']
    df_private['created_at'] = pd.to_datetime(temp_df['created_at'])
    monthago = datetime.now() - timedelta(days = 30)
    df_private['day_month_ago'] = monthago
    df_private['created_at']=df_private['created_at'].apply(lambda x:x.replace(tzinfo=None))
    df_private['month_ago']= (df_private['created_at'] > df_private['day_month_ago'])

    df_private = df_private.loc[df_private['month_ago'],:]
    #changes ID's to list for next function call
    recent_ids = df_private['id'].astype(str).tolist()

    # Make the request
    oauth = OAuth1Session(
        consumer_key,
        client_secret=consumer_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_token_secret,
    )

    n = len(recent_ids)

    _id = []
    _impressions = []
    _clicks = []
    _created_at =[]
    _author_id=[]
    _text = []


    for i in range(0,n):
        if((n+1)%299==0):
            print("Too many tweets. I will sleep for 15 minutes")
            sleep(1000)
            print("I am awake now")
        params = {"ids": recent_ids[i], "tweet.fields": "created_at","tweet.fields":"public_metrics","tweet.fields":"author_id","tweet.fields":"text","tweet.fields": "non_public_metrics"}
        # Make the request
        oauth = OAuth1Session(
            consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=access_token,
            resource_owner_secret=access_token_secret,
        )

        response = oauth.get(
            "https://api.twitter.com/2/tweets", params=params
        )

        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(response.status_code, response.text)
            )
        json_response = response.json()
        print(json_response)
        _id.append(json_response['data'][0]['id'])
        _impressions.append(json_response['data'][0]['non_public_metrics']["impression_count"])
        _clicks.append(json_response['data'][0]['non_public_metrics']['user_profile_clicks'])

    original = np.array([_id,_impressions,_clicks])
    rotated = zip(*original[::-1])
    non_public_df = pd.DataFrame(rotated,columns=['clicks','impressions','id'])
    print(non_public_df.head())

    print(temp_df)

    result_df = pd.merge(temp_df, non_public_df, how='outer', on="id")
    result_df['total_engagement'] = result_df['retweet_count'] + result_df['reply_count'] + result_df['like_count'] + pd.to_numeric(result_df['clicks'])
    result_df["engagement_percent"] = result_df["total_engagement"]/pd.to_numeric(result_df["impressions"])*100
    #result_df["sentiment"] = result_df['text'].apply(lambda x:sia.polarity_scores(x)['compound'])
    result_df.head()

    #cleans tweets
    
    result_df['text']=result_df['text'].apply(lambda x: x.replace('"',''))
    result_df['text']=result_df['text'].apply(lambda x: x.replace("'",''))

    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+Authserver+';DATABASE='+Authdatabase+';UID='+Authusername+';PWD='+Authpassword)
    cursor = cnxn.cursor()

    for index, row in result_df.iterrows():
        if(str(row['impressions']) == 'nan'): # Avoids putting nulls into the SQL from non-public data.
            cursor.execute(("UPDATE {table_name} WITH (UPDLOCK, SERIALIZABLE) SET created_at='{created_at}', lang='{lang}', tweet='{text}', created_date='{created_date}', source='{source}', retweet_count='{retweet_count}', reply_count='{reply_count}', like_count='{like_count}', quote_count='{quote_count}', author_id='{author_id}', URL='{URL}'  WHERE Id = {Id};"
            "IF @@ROWCOUNT = 0 "
            "BEGIN"
            "    INSERT INTO {table_name} (Id,created_at,created_date,lang,tweet,source,retweet_count,reply_count,like_count,quote_count,author_id,URL) VALUES({Id},'{created_at}','{created_date}','{lang}','{text}','{source}','{retweet_count}','{reply_count}','{like_count}','{quote_count}','{author_id}','{URL}');"
            "END").format(table_name=stable_name,Id=row.id,created_at=row.created_at,created_date=row.created_date,lang=row.lang,text=row.text,source=row.source,retweet_count=row.retweet_count,reply_count=row.reply_count,like_count=row.like_count,quote_count=row.quote_count,author_id=row.author_id,URL=row.URL,clicks=row.clicks,impressions=row.impressions,total_engagement=row.total_engagement,engagement_percent=row.engagement_percent))
        else:
            cursor.execute(("UPDATE {table_name} WITH (UPDLOCK, SERIALIZABLE) SET created_at='{created_at}', lang='{lang}', tweet='{text}', created_date='{created_date}', source='{source}', retweet_count='{retweet_count}', reply_count='{reply_count}', like_count='{like_count}', quote_count='{quote_count}', author_id='{author_id}', URL='{URL}', clicks='{clicks}', impressions='{impressions}', total_engagement='{total_engagement}', engagement_percent='{engagement_percent}'  WHERE Id = {Id};"
            "IF @@ROWCOUNT = 0 "
            "BEGIN"
            "    INSERT INTO {table_name} (Id,created_at,created_date,lang,tweet,source,retweet_count,reply_count,like_count,quote_count,author_id,URL,clicks,impressions,total_engagement,engagement_percent) VALUES({Id},'{created_at}','{created_date}','{lang}','{text}','{source}','{retweet_count}','{reply_count}','{like_count}','{quote_count}','{author_id}','{URL}','{clicks}','{impressions}','{total_engagement}','{engagement_percent}');"
            "END").format(table_name=stable_name,Id=row.id,created_at=row.created_at,created_date=row.created_date,lang=row.lang,text=row.text,source=row.source,retweet_count=row.retweet_count,reply_count=row.reply_count,like_count=row.like_count,quote_count=row.quote_count,author_id=row.author_id,URL=row.URL,clicks=row.clicks,impressions=row.impressions,total_engagement=row.total_engagement,engagement_percent=row.engagement_percent))
        cursor.commit()
    cursor.close()
    print("Finished process: ",username)
    return App_rate_limit