import Twitter_Func as hello
import pyodbc
import pandas as pd

Authserver = 
Authdatabase = 
Authusername =
Authpassword =

acnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+Authserver+';DATABASE='+Authdatabase+';UID='+Authusername+';PWD='+ Authpassword)
cursor = acnxn.cursor()

Twitter_Credentials = pd.read_sql_query('SELECT * FROM INSERT AUTH TABLE',acnxn)

print(Twitter_Credentials)
cursor.close()
print("here")


App_rate_limit = 0
n = len(Twitter_Credentials.index)

for i in range(0,n):
    user_id = Twitter_Credentials['user_id'][i]
    username = Twitter_Credentials['username'][i]   
    ##Pass in the following arguments
    BEARER_TOKEN=Twitter_Credentials['BEARER_TOKEN'][i]
    #Set up consumer key
    consumer_key = Twitter_Credentials['consumer_key'][i]
    consumer_secret = Twitter_Credentials['consumer_secret'][i]

    #Set up API access token
    access_token = Twitter_Credentials['access_token'][i]
    access_token_secret = Twitter_Credentials['access_token_secret'][i]

    #set servername and login informatoin 
    stable_name = Twitter_Credentials['table_name'][i]

    App_rate_limit = hello.daily(user_id,username,BEARER_TOKEN,consumer_key,consumer_secret,access_token,access_token_secret,Authserver,Authdatabase,Authusername,Authpassword,stable_name,App_rate_limit)
    print("App rate limit:", App_rate_limit)