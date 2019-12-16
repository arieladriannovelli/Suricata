import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/sentiment/89b16b2d2a6f.json"

from google.cloud import translate
client = translate.Client()

import tweepy
auth = tweepy.auth.OAuthHandler('5oQnztMuTHhAZs31ScKqSy5cG', '1ACDELTfXyGS0Ugf7duI5keadjVHUJebxoBglmsUH5ILcKxIzk')
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)
    
import paralleldots
import json
paralleldots.set_api_key("zMWZNCUZvgbWpPUAVYxGgO1pY6BKT3qvvyVY8wGVaWs")

import sys
import jsonpickle

import time

searchQuery = 'Eugenia Vidal'

maxTweets = 1000
#maxTweets = 10000000 # Some arbitrary large number

tweetsPerQry = 30  # this is the max the API permits
fName = 'tweetsvidal.txt' # We'll store the tweets in a text file.


#sinceId= None

import pyodbc 
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=190.210.180.120;'
                      'DATABASE=OPINOWEB;'
                      'UID=sa;'
                      'PWD=Quebueno2')
cursor = conn.cursor()
cursor.execute("SELECT isnull (Max(Id),0) FROM sur_listener where id_listener='Vidal'")
sinceId= cursor.fetchone()[0]
#sinceId= 1104537753193246720

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -1

tweetCount = 0
print("Downloading max {0} tweets".format(maxTweets))
with open(fName, 'w') as f:
    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                if (not sinceId):                                        
                    new_tweets = api.search(q=searchQuery, lang='es', tweet_mode='extended', count=tweetsPerQry)
                else:
                    new_tweets = api.search(q=searchQuery, lang='es', tweet_mode='extended', count=tweetsPerQry,
                                            since_id=sinceId)
            else:
                if (not sinceId):
                    new_tweets = api.search(q=searchQuery, lang='es', tweet_mode='extended', count=tweetsPerQry,
                                            max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=searchQuery, lang='es', tweet_mode='extended', count=tweetsPerQry,
                                            max_id=str(max_id - 1),
                                            since_id=sinceId)
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                f.write(jsonpickle.encode(tweet._json, unpicklable=False) +
                        '\n')
            tweetCount += len(new_tweets)
            print("Downloaded {0} tweets".format(tweetCount))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break

print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))

import json
import string
import re,string

def strip_links(text):
    link_regex    = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links         = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')    
    return text

def strip_all_entities(text):
    entity_prefixes = ['@','#',';','.','!','?']
    #entity_prefixes = [';']
    
    for separator in  string.punctuation:
        if separator not in entity_prefixes :
            text = text.replace(separator,' ')
    words = []
    for word in text.split():
        word = word.strip()
        if word:
            if word[0] not in entity_prefixes:
                words.append(word)
    return ' '.join(words)

#from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#analyser = SentimentIntensityAnalyzer()

from googletrans import Translator
translator = Translator()

import random

def sentiment_analyzer_scores(sentence):
    score = analyser.polarity_scores(sentence)
    #print("{:-<40}".format(str(score)))
    #return score
    return score['compound']

with open('tweetsvidal.txt', 'r') as f:
    for line in f:
        tweet = json.loads(line) # load it as Python dict

        if tweet['place'] != None :
            country = str(tweet['place']['country'])
        else :
            country = ''
            
        if tweet['coordinates'] != None :
            longitude = str(tweet['coordinates']['coordinates'][0])
            latitude = str(tweet['coordinates']['coordinates'][1])
        else :
            longitude=''
            latitude=''
        
        full_text = str(tweet['full_text'])
        full_text = full_text.replace ("'","")
        
        user_location = str(tweet["user"]["location"])
        user_location = user_location.replace ("'","")
        
        user_screen_name = str(tweet["user"]["screen_name"])
        user_screen_name = user_screen_name.replace("'","")
        
        scores = 0;
        
        Negative = 0;
        Neutral = 0;
        Positive = 0;

        Education=0;
        Health=0;
        Economy=0;
        Security=0;
        Corruption=0;
        Work=0;
        Transport=0;
        LivingPlace=0;
        
        Happy =0;
        Angry =0;
        Excited =0;
        Sad = 0;
        Fear = 0;
        Bored =0;
  
        if random.randint(1,26) == 25:
        
            # Wait for 1 second
            time.sleep(2) 
    
            #Translate (Google Translate API)
            text_translated = client.translate(full_text)['translatedText']
            print (full_text)
        
            #Emotion (Paralleldots API)
            response=paralleldots.emotion(text_translated,'en')
            print (response)
            
            Happy = response["emotion"]["Happy"]
            Angry = response["emotion"]["Angry"]
            Excited = response["emotion"]["Excited"]
            Sad = response["emotion"]["Sad"]
            Fear = response["emotion"]["Fear"]
            Bored = response["emotion"]["Bored"]
            
            #Sentiment (Paralleldots API)
            response=paralleldots.sentiment(text_translated,'en')
            print (response)
            Negative = response["sentiment"]["negative"]
            Neutral = response["sentiment"]["neutral"]
            Positive = response["sentiment"]["positive"]
        
            #Categories (Paralleldots API)
            category  ={ "education": [], "health": [] , "economy": [] , "security": [] , "corruption": [] , "work": [] , "transport": [] , "living place": []}        
            response=paralleldots.custom_classifier(text_translated,category)
            print(response)
                                
            for i in range(0, 8):
                Aux_1 = response["taxonomy"][i]["tag"]
                Aux_2 = response["taxonomy"][i]["confidence_score"]
                if Aux_1=="education":
                    Education = Aux_2;
                if Aux_1=="health":
                    Health = Aux_2;
                if Aux_1=="economy":
                    Economy = Aux_2;
                if Aux_1=="security":
                    Security = Aux_2;
                if Aux_1=="corruption":
                    Corruption = Aux_2;
                if Aux_1=="work":
                    Work = Aux_2;
                if Aux_1=="transport":
                    Transport = Aux_2;
                if Aux_1=="living place":
                    LivingPlace = Aux_2;
        
        
        cursor.execute("INSERT INTO sur_listener VALUES ('Vidal'," + str(tweet['id']) + ",'" + str(tweet['created_at']) + "',N'" + full_text + "','" + str(tweet['favorite_count']) + "','" + str(tweet['retweet_count']) + "','" + str(tweet['in_reply_to_user_id']) + "','" + user_location + "','" + user_screen_name +"','" + str(tweet["user"]["friends_count"]) +"','" + str(tweet["user"]["followers_count"]) + "','" + str(tweet["lang"]) + "','" + country + "','" + latitude + "','" + longitude + "','" + str(scores) + "','" + str(Happy) + "','" + str(Angry) + "','" + str(Excited) + "','" + str(Sad) + "','" + str(Fear) + "','" + str(Bored) + "','" + str(Negative) + "','" + str(Neutral) + "','" + str(Positive) + "','" + str(Education) + "','" + str(Health) + "','" + str(Economy) + "','" + str(Security) + "','" + str(Corruption) + "','" + str(Work) + "','" + str(Transport) + "','" + str(LivingPlace) + "','','')")
        
        conn.commit()
        
#This        
cursor.execute("insert into sur_listener_wc Select Id, Created_At, substring (dbo.fn_CleanTextV2(dbo.fn_CleanAndTrim(dataitem,'','',1)),1,50), 'Vidal' from (select * from sur_listener where id_listener='Vidal' and id>(select isnull(max(id),0) from sur_listener_wc where id_listener='Vidal')) x cross apply dbo.splitString(x.Full_Text,' ') Y option (maxrecursion 0) ")
conn.commit()

cursor.execute("delete from sur_listener_wc where id_listener='Vidal' and word in (select word from sur_listener_wc_filters where id_listener='Vidal')")
conn.commit()

cursor.execute("delete from sur_listener_wc where id_listener='Vidal' and word like '@%'")
conn.commit()
        
conn.close()
print ("Done!")

