from dotenv import load_dotenv
import os
import pandas as pd
import function
import db_service
import isodate


load_dotenv()  # take environment variables from .env.


API_KEY = os.getenv('youtube_key2')
host = os.getenv('host')
port = os.getenv('db_port')
username = os.getenv('db_username')
password = os.getenv('db_password')

channelId = "UCCezIgC97PvUuR4_gbFUs5g"
# channelId = "UCJoG46MzkLfd_yTTEApw5fA"

data = None

try:
    video_ids = function.get_video_ids(API_KEY, channelId)
    data = function.get_video_details(API_KEY, video_ids)
    #convert duration to secounds
    data['duration'] = data['duration'].apply(lambda x: isodate.parse_duration(x))
except Exception as e:
    print(e)
else:
    conn = db_service.connect_to_db(host,port,username,password)

    db_service.create_table(conn)

    for i, row in data.iterrows():
        if db_service.check_if_video_exists(conn,row['video_id']):
            db_service.update_vid(conn,row['video_id'],row['channelTitle'], row['title'],row['description'],row['tags'],row['viewCount'],row['likeCount'],row['commentCount'],row['publishedAt'],row['duration'])
        else:
            db_service.insert_vid(conn,row['video_id'],row['channelTitle'], row['title'],row['description'],row['tags'],row['viewCount'],row['likeCount'],row['commentCount'],row['publishedAt'],row['duration'])

    print('Data loaded Successfully')