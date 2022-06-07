from dotenv import load_dotenv
import os
import pandas as pd
import function


load_dotenv()  # take environment variables from .env.


API_KEY = os.getenv('youtube_key')

channelId = "UCCezIgC97PvUuR4_gbFUs5g"
# channelId = "UCJoG46MzkLfd_yTTEApw5fA"


df = pd.DataFrame(columns=['video_id','channelTitle','publish_date','title','description','liveBroadcastContent'])

data = function.get_vidoes(df, API_KEY,channelId)

print(data)