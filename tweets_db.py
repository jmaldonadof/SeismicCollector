from peewee import Model, MySQLDatabase,\
					IntegerField as Int, \
					CharField as Char, \
					TextField as Text, \
					BooleanField as Bool, \
					BigIntegerField as BigInt, \
					DateTimeField as DateTime, \
					ForeignKeyField as ForeignKey, \
					FloatField as Float
import credentials
import datetime
import time

#Credentials to connect with database
db = MySQLDatabase(credentials.db_name, user=credentials.db_user, passwd=credentials.db_pass, charset='utf8')

def getDB():
	return db

class BaseModel(Model):
	class Meta:
		database = db

class Keyword(BaseModel):
	word = Char(index = True)

class User(BaseModel):
	user_id = BigInt(primary_key=True)
	location = Char(null=True)
	name = Char()
	screen_name = Char()
	profile_image_url = Char()
	added_at = DateTime(index = True)
	
class Tweet(BaseModel):
	tweet_id = BigInt(primary_key=True)
	text = Char(max_length=140)
	in_reply_to_status_id = BigInt(null=True)
	favorite_count = Int()
	coordinates = Char(null=True)
	entities = Text(null=True)
	retweet_count = Int()
	is_retweet = Bool()
	retweet_of = ForeignKey('self', related_name='retweets', null=True)
	user = ForeignKey(User, related_name='tweets', index = True)
	lang = Char()
	created_at = DateTime(index = True)
	keywords = Char(index = True)
	
class TweetLocation(BaseModel):
	tweet_id = ForeignKey(Tweet, related_name='resolved_location', index = True)
	country_code = Char(max_length=2, null=True)
	admin1 = Char(max_length=20, null=True)
	admin2 = Char(max_length=80, null=True)
	admin3 = Char(max_length=20, null=True)
	admin4 = Char(max_length=20, null=True)
	longitude = Float(null=True)
	latitude = Float(null=True) 
	
class UserLocation(BaseModel): 
	user_id = ForeignKey(User, related_name='resolved_location', index = True)
	country_code = Char(max_length=2, null=True)
	admin1 = Char(max_length=20, null=True)
	admin2 = Char(max_length=80, null=True)
	admin3 = Char(max_length=20, null=True)
	admin4 = Char(max_length=20, null=True)
	longitude = Float(null=True)
	latitude = Float(null=True) 

def create_tables():
	try:
		User.create_table()
	except Exception, e:
		pass
	try:
		Tweet.create_table()
	except Exception, e:
		pass
	try:
		TweetLocation.create_table()
	except Exception, e:
		print e
		pass
	try:
		UserLocation.create_table()
	except Exception, e:
		print e
		pass


def tryConnect():
	try:
		db.connect()
		return db
	except Exception as e:
		print 'Error! ' + str(e)
		tryConnect()

def tryDisconnect():
	try:
		db.close()
	except Exception as e:
		print 'Error!' + str(e)

		
def save_user(user):
	#Search if the user exist in database
	user_id = user['id']
	saved_user = User.select().where(User.user_id == user_id)
	
	#If user doesn't exist, it is created
	if not saved_user.exists():
		location = user['location']
		name = user['name']
		screen_name = user['screen_name']
		profile_image_url = user['profile_image_url']
		fdate = datetime.datetime.now()
		user = User(user_id=user_id,
					location=location,
					name=name,
					screen_name=screen_name,
					profile_image_url = profile_image_url,
					added_at = fdate)
		try:
			user.save(force_insert=True)
			return user
		except Exception, e:
			print 'Error! ' + str(e)
			return 0
	
	return saved_user
		
def save_tweet(tweet):	
	#Save the user		
	user = save_user(tweet['user'])

	#If tweet is a retweet save the original tweet too
	if 'retweeted_status' in tweet:
		original_tweet = tweet['retweeted_status']
		save_tweet(original_tweet)
	
	#Search if the tweet exists in database
	tweet_id = tweet['id']
	saved_tweet = Tweet.select().where(Tweet.tweet_id == tweet_id)
				
	#If tweet doesn't exist, it is created
	if not saved_tweet.exists():		
		text = tweet['retweeted_status']['text'] if 'retweeted_status' in tweet else tweet['text']
		in_reply_to_status_id = tweet['in_reply_to_status_id']
		favorite_count = tweet['favorite_count']
		coordinates = str(tweet['coordinates'])
		entities = str(tweet['entities'])
		retweet_count = tweet['retweet_count']
		is_retweet = True if 'retweeted_status' in tweet else False
		retweet_of = tweet['retweeted_status']['id'] if is_retweet else None
		lang = tweet['lang']
		created_at = datetime.datetime.utcfromtimestamp(tweet['created_at']) #Receive a timestamp
		keywords = tweet['keywords']		
		
		t = Tweet(tweet_id=tweet_id,
				  text=text,
				  in_reply_to_status_id=in_reply_to_status_id,
				  favorite_count=favorite_count,
				  coordinates=coordinates,
				  entities=entities,
				  retweet_count=retweet_count,
				  is_retweet=is_retweet,
				  retweet_of=retweet_of,
				  user=user,
				  lang=lang,
				  keywords = keywords,
				  created_at=created_at)

		try:
			t.save(force_insert=True)
			return 1
		except Exception, e:
			print 'Error! ' + str(e)
			return 0
		

if __name__ == '__main__':
	create_tables()
	

"""
Book.create_table()
book = Book(author="me", title='Peewee is cool')
book.save()
for book in Book.filter(author="me"):
	print book.title
"""
