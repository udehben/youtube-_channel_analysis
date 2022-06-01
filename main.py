from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()  # take environment variables from .env.

API_KEY = (os.getenv('youtube_key'))

CHANNELID = "UCCezIgC97PvUuR4_gbFUs5g"

param = {'key': API_KEY,
         'channelId': CHANNELID,
         'part':'snippet'}

response = requests.get('https://www.googleapis.com/youtube/v3/search', params=param).json()
print(response)