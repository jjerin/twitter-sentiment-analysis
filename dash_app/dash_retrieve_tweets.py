import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import credentials

import re
import tweepy
import mysql.connector
import pandas as pd
from textblob import TextBlob
import time

from tweepy import StreamListener
from tweepy import Stream

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H6("Enter keyword to search twitter!"),
    html.Div(["Input: ",
              dcc.Input(id='my-input', value='Enter Keyword', type='text', debounce=True)]),
              dcc.Loading(
                    id="loading-1",
                    children=[html.Div([html.Div(id="my-output")])],
                    type="circle",
              ),
    html.Br(),
    # html.Div(id='my-output'),
    dcc.Link('Track Realtime', href='http://localhost:8051/'),

])

TRACK_WORDS = ['Pence']
TABLE_NAME = 'TSTA_Tweets'
TABLE_ATTRIBUTES = "id_str VARCHAR(255), keyword VARCHAR(255), created_at DATETIME, text VARCHAR(255),             polarity INT, subjectivity INT, user_created_at VARCHAR(255), user_location VARCHAR(255),             user_description VARCHAR(255), user_followers_count INT, longitude DOUBLE, latitude DOUBLE,             retweet_count INT, favorite_count INT"

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="jerin",
    database="TwitterDB",
    use_pure=True,
    charset = 'utf8'
)
    
def connectDB():
    print('connectDB called') 

    if mydb.is_connected():
        '''
        Check if this table exits. If not, then create a new one.
        '''
        print('is_connected called') 

        mycursor = mydb.cursor()
        mycursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(TABLE_NAME))
        if mycursor.fetchone()[0] != 1:
            mycursor.execute("CREATE TABLE {} ({})".format(TABLE_NAME, TABLE_ATTRIBUTES))
            mydb.commit()
        mycursor.close()    
    print('connectDB complete')

def cleanupTable():
    print('cleanupTable called') 

    if mydb.is_connected():
        '''
        Check if this table exits. If not, then create a new one.
        '''
        print('is_connected called') 

        mycursor = mydb.cursor()
        mycursor.execute("delete from {} ".format(TABLE_NAME))
        mydb.commit()
        
        
        mycursor = mydb.cursor()
        mycursor.execute("update back_up set daily_tweets_num = 0, impressions = 0 where id = 1 ")
        mydb.commit()

        mycursor.close()    
    print('connectDB complete')
   
    
def retrieveTweets(keyword):
    print('retrieveTweets called. keyword: ', keyword) 
    
    connectDB()
    
    # cleanupTable()

    auth  = tweepy.OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    del TRACK_WORDS[:]
    if keyword != 'Enter Keyword':
        TRACK_WORDS.append(keyword)
    
    
    if TRACK_WORDS: 
        cleanupTable()
        MyStreamListener.start_time = None
        myStreamListener = MyStreamListener()
        myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
        # TRACK_WORDS = [keyword]
        myStream.filter(languages=["en"], track = TRACK_WORDS)
        # Close the MySQL connection as it finished
        # However, this won't be reached as the stream listener won't stop automatically
        # Press STOP button to finish the process.
        # mydb.close()
        print('retrieveTweets complete')




# Override tweepy.StreamListener to add logic to on_status
class MyStreamListener(StreamListener):
    '''
    Tweets are known as “status updates”. So the Status class in tweepy has properties describing the tweet.
    https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
    '''
    
    tweet_counter = 0
    stop_at = 20
    start_time = None

    def __init__(self, time_limit=10):
        super().__init__()
        # print('__init__ called') 
        # self.start_time = time.time()
        # self.limit = time_limit
        # super(MyStreamListener, self).__init__()   
        
    def on_status(self, status):
        '''
        Extract info from tweets
        '''
        print('on_status  called') 
        
        if status.retweeted:
            # Avoid retweeted info, and only original tweets will be received
            return True
        # Extract attributes from each tweet
        id_str = status.id_str
        created_at = status.created_at
        text = deEmojify(status.text)    # Pre-processing the text  
        sentiment = TextBlob(text).sentiment
        polarity = sentiment.polarity
        subjectivity = sentiment.subjectivity
        
        user_created_at = status.user.created_at
        user_location = deEmojify(status.user.location)
        user_description = deEmojify(status.user.description)
        user_followers_count =status.user.followers_count
        longitude = None
        latitude = None
        if status.coordinates:
            longitude = status.coordinates['coordinates'][0]
            latitude = status.coordinates['coordinates'][1]
            
        retweet_count = status.retweet_count
        favorite_count = status.favorite_count
        
        MyStreamListener.tweet_counter += 1
        
        # print(status.text)
        # print("Long: {}, Lati: {}".format(longitude, latitude))
        
        # Store all data in MySQL
        if mydb.is_connected():
            mycursor = mydb.cursor()
            sql = "INSERT INTO {} (id_str, keyword, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(TABLE_NAME)
            val = (id_str, TRACK_WORDS[0], created_at, text, polarity, subjectivity, user_created_at, user_location,                 user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
            mycursor.execute(sql, val)
            mydb.commit()
            mycursor.close()
        
        # if MyStreamListener.tweet_counter < MyStreamListener.stop_at:
        #   return True
        # else:
        #   print('Max num reached = ' + str(MyStreamListener.tweet_counter))
        #   return False

        # if (time.time() - self.start_time) < self.limit:
        #     return True
        # else:
        #     print('Time elapsed.')
        #     return False
    
    
    def on_error(self, status_code):
        '''
        Since Twitter API has rate limits, stop srcraping data as it exceed to the thresold.
        '''
        print('on_error called. status_code: ',status_code)
        if status_code == 420:
            # return False to disconnect the stream
            return False

def clean_tweet(self, tweet): 
    ''' 
    Use sumple regex statemnents to clean tweet text by removing links and special characters
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])                                 |(\w+:\/\/\S+)", " ", tweet).split()) 
def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None



@app.callback(
    Output(component_id='my-output', component_property='children'),
    Input(component_id='my-input', component_property='value')
)
def update_output_div(input_value):
    print('update_output_div called. input_value: ', input_value) 
    retrieveTweets(input_value)
    
    if input_value == 'Enter Keyword':
        return ''
    return 'Output: {}'.format(input_value)


if __name__ == '__main__':
    app.run_server(debug=True)