import json, time

import tweepy
import Publisher as publisher

# Authenticate
from tweepy import StreamListener

auth = tweepy.OAuthHandler("f2P1qTi4YioChDeALucTREBfX", "Lidx9DszAoVpzRo6QGyJwUhzceZ5hOxe7SkMI9xw5EulKMzmaS")
auth.set_access_token("153918738-SQi9h0I8UWA4ABmZOyiZyWP8ilD39kfAXQfGqSFT", "T5nuktfWRiRICE5RmvAalGuDOILJ3AB9pMPQn98vrzkZC")

# Configure to wait on rate limit if necessary
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

# Hashtag list
lst_hashtags = ["فودافون"]


class TweetListener(StreamListener):

    def on_status(self, data):
        # When receiving a tweet: send it to pubsub
        #data = self.createJSON(data._json)
        publisher.write_to_pubsub(data._json)
        #print(self.createJSON(data._json).decode())
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False

    def createJSON(self, data):
        json_str = json.dumps({
            'uniqueID': data["id"],
            'text': data["text"],
            "user_name": data["user"]["screen_name"],
            "created_at": time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        },ensure_ascii=False)
        return json_str.encode('utf8')


if __name__ == '__main__':

# Make an instance of the class
    l = TweetListener()

# Start streaming
    stream = tweepy.Stream(auth, l, tweet_mode='extended')
    stream.filter(track=lst_hashtags)