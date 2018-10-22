import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket

consumer_key = '4tHBR9dauSEV0KU8vP8KH8SJB'
consumer_secret = 'JqzroHwIAAy12H1CKpWs9MkGMqrgaWPbtpF6RVrkI7ZynfrKFV'

access_token = '1044608872391028738-F2eYK2zZGkz7IgEceVEjpheiSPXNeB'
access_token_secret = 'O3s3hXhttSlPOMh4tkMSimYrpoRnSErHguvPmXx71z5mW'

class StdOutListener(StreamListener):

    def _init_(self):
        super()._init_()
        self.counter = 0
        self.limit = 10


    def on_status(self,status):
        
        try:
            print(status.text)
            self.counter += 1
            if self.counter < self.limit:
                return True
            else:
                stream.disconnect()
                
        except BaseException as e:
            print('failed on_status,',str(e))
            time.sleep(5)

        return True

auth = tweepy.auth.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
stream=Stream(auth,StdOutListener())
stream.filter(track=['Trump'])
