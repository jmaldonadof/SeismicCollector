#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tweepy
import tweets_db 
import json
import credentials
import pika
import time

# Set encoding to utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# Disable alerts from urllib3
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

# Set to True if you want to enqueue the data
enqueue = True

# List of keywords to collect
keywords = ['temblor','terremoto','earthquake','quake','gempa','lindol','scossa','deprem']


def extract_keywords(text):
	words = []
	for keyword in keywords:
		if keyword in text.lower():
			words.append(keyword)
	return words

class CustomStreamListener(tweepy.StreamListener):
	""" A listener handles tweets are the received from the stream. 
	This listener saves the data into a database and also in a queue
	for being consumed by another application.
	"""
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()
		
		if enqueue:
			# Setup rabbitMQ Connection
			connection = pika.BlockingConnection(
				pika.ConnectionParameters(host='localhost')
			)
			self.channel = connection.channel()
	
			# Set max queue size
			args = {"x-max-length": 2000}
			
			# Declare the queue
			self.channel.queue_declare(queue='twitter_topic_feed', arguments=args)
			
	def on_status(self, status):
		info = {}
		info['id'] = status.id
		info['text'] = status.text
		info['coordinates'] = status.coordinates
		info['entities'] = status.entities
		info['retweet_count'] = status.retweet_count
		info['favorite_count'] = status.favorite_count
		info['in_reply_to_status_id'] = status.in_reply_to_status_id
		info['lang'] = status.lang
		info['created_at'] = time.mktime(status.created_at.timetuple())
		info['keywords'] = str(extract_keywords(status.text))
		info['user'] = {}
		info['user']['id'] = status.user.id
		info['user']['name'] = status.user.name
		info['user']['screen_name'] = status.user.screen_name
		info['user']['location'] = status.user.location
		info['user']['profile_image_url'] = status.user.profile_image_url
		
		if hasattr(status, 'retweeted_status'):
			info['retweeted_status'] = {}
			info['retweeted_status']['id'] = status.retweeted_status.id
			info['retweeted_status']['text'] = status.retweeted_status.text
			info['retweeted_status']['coordinates'] = status.retweeted_status.coordinates
			info['retweeted_status']['entities'] = status.retweeted_status.entities
			info['retweeted_status']['retweet_count'] = status.retweeted_status.retweet_count
			info['retweeted_status']['favorite_count'] = status.retweeted_status.favorite_count
			info['retweeted_status']['in_reply_to_status_id'] = status.retweeted_status.in_reply_to_status_id
			info['retweeted_status']['lang'] = status.retweeted_status.lang
			info['retweeted_status']['created_at'] = time.mktime(status.retweeted_status.created_at.timetuple())
			info['retweeted_status']['keywords'] = str(extract_keywords(status.retweeted_status.text))
			info['retweeted_status']['user'] = {}
			info['retweeted_status']['user']['id'] = status.retweeted_status.user.id
			info['retweeted_status']['user']['name'] = status.retweeted_status.user.name
			info['retweeted_status']['user']['screen_name'] = status.retweeted_status.user.screen_name
			info['retweeted_status']['user']['location'] = status.retweeted_status.user.location
			info['retweeted_status']['user']['profile_image_url'] = status.retweeted_status.user.profile_image_url
		
		# Print tweet in screen
		print str(status.id) + ' - ' + status.text
			
		if enqueue:	
			# Enqueue the tweet
			self.channel.basic_publish(exchange='',
	                                    routing_key='twitter_topic_feed',
	                                    body=json.dumps(info))
			
		# Save tweet into the database
		if not tweets_db.save_tweet(info):
			print 'Error, tweet was not saved'			

		return True
	
	def on_error(self, status):
		print status


# Authentication
auth = tweepy.OAuthHandler(credentials.consumer_key, credentials.consumer_secret)
auth.set_access_token(credentials.access_token, credentials.access_token_secret)
api = tweepy.API(auth)

# Initialize the stream collector
stream = tweepy.Stream(auth, CustomStreamListener(api))
while True:
	try:
		stream.filter(track=keywords)
	except Exception, e:
		print e
		continue