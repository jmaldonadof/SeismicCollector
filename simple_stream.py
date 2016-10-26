#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tweepy
import json
import credentials
import time
import calendar

# Set logging service
import logging
log_filename = 'simple_stream.log'
log_format_style = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(filename=log_filename, format=log_format_style, level=logging.INFO)

# Set encoding to utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# Disable alerts from urllib3
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

# List of keywords to collect
keywords = ['temblor','terremoto', 'earthquake']

# Bounding Box Coordinates
boundingBox_SW_Long = -76.8507235
boundingBox_SW_Lat = -55.1671700
boundingBox_NE_Long = -66.6756380
boundingBox_NE_Lat = -17.5227345

def extract_keywords(text):
	words = []
	for keyword in keywords:
		if keyword in text.lower():
			words.append(keyword)
	return words

class CustomStreamListener(tweepy.StreamListener):
	""" A listener handles tweets received from the stream. 
	This listener prints tweet info in json format on screen.
	"""
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()
			
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
		info['created_at'] = calendar.timegm(status.created_at.timetuple()) #Send a timestamp 
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
		print json.dumps(info)
			
		return True
	
	def on_error(self, status):
		logging.error(status)


def stream_filter_by_keywords(stream):
	stream.filter(track=keywords)
	
def stream_filter_by_location(stream):
	stream.filter(locations=[boundingBox_SW_Long, boundingBox_SW_Lat, boundingBox_NE_Long, boundingBox_NE_Lat])

# Authentication
auth = tweepy.OAuthHandler(credentials.consumer_key, credentials.consumer_secret)
auth.set_access_token(credentials.access_token, credentials.access_token_secret)
api = tweepy.API(auth)

# Initialize the stream collector
stream = tweepy.Stream(auth, CustomStreamListener(api))
while True:
	try:
		stream_filter_by_location(stream)
	except Exception, e:
		logging.error(e)
		continue