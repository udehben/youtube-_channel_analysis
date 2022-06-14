from dotenv import load_dotenv
import os
import pandas as pd
import function
import db_service
from tqdm import tqdm


load_dotenv()  # take environment variables from .env.


API_KEY = os.getenv('youtube_key')
host = os.getenv('host')
port = os.getenv('db_port')
username = os.getenv('db_username')
password = os.getenv('db_password')

channelId = "UCCezIgC97PvUuR4_gbFUs5g"
# channelId = "UCJoG46MzkLfd_yTTEApw5fA"


df = pd.DataFrame(columns=['video_id','channelTitle','publish_date','title','description','liveBroadcastContent'])
data = None

try:
    data = function.get_vidoes(df, API_KEY,channelId)
except Exception as e:
    print(e)
else:
    print('Data loaded Successfully')

conn = db_service.connect_to_db(host,port,username,password)

db_service.create_table(conn)

for i, row in tqdm(data.iterrows()):
    if db_service.check_if_video_exists(conn,row['video_id']):
        db_service.update_vid(conn,row['video_id'],row['title'], row['description'],row['duration'],row['publish_date'],row['viewCount'],row['likeCount'],row['commentCount'],row['liveBroadcastContent'])
    else:
        db_service.insert_vid(conn,row['video_id'],row['title'], row['description'],row['duration'],row['publish_date'],row['viewCount'],row['likeCount'],row['commentCount'],row['liveBroadcastContent'])