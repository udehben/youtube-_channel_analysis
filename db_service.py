import psycopg2 as ps
import pandas as pd

# Connect to your postgres DB
def connect_to_db(host,port,username,password):
    try:
        conn = ps.connect(host=host,user=username,password=password,port=port)
    except Exception as e:
        print(e)
    else:
        print('Connected Successfully!')
    return conn

# create table
def create_table(conn):
    try:
        curr = conn.cursor()
        create_table_command = ('''CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(100) PRIMARY KEY,
            channel_title TEXT NOT NULL,
            title TEXT NOT NULL,
            "description" TEXT NOT NULL,
            duration TIME NOT NULL,
            publish_date DATE NOT NULL DEFAULT CURRENT_DATE,
            view_count INTEGER NOT NULL,
            like_count INTEGER NOT NULL,
            comment_count INTEGER NOT NULL,
            tags VARCHAR NOT NULL)''')
        curr.execute(create_table_command)
    except Exception as e:
        print(e)
    else:
        conn.commit()

# check to see if video exists
def check_if_video_exists(conn,video_id):
    try:
        curr = conn.cursor()
        query = ('''SELECT video_id FROM videos WHERE video_id = %s''')
        curr.execute(query,(video_id,))
    except Exception as e:
        print(e)
    else:
        return(curr.fetchone() is not None)

# update video details on the db
def update_vid(conn,video_id,channelTitle,title,description,tags,viewCount,likeCount,commentCount,publishedAt,duration):
    try:
        curr = conn.cursor()
        update_comand = ('''UPDATE videos
                SET channel_title = %s,
                title = %s,
                "description" = %s,
                duration = %s,
                publish_date = %s,
                view_count = %s,
                like_count = %s,
                comment_count = %s,
                tags = %s
                WHERE video_id = %s''')
        columns_to_update = (channelTitle,title,description,duration,publishedAt,viewCount,likeCount,commentCount,tags,video_id)
        curr.execute(update_comand,(columns_to_update))
    except Exception as e:
        print(e)
    else:
        conn.commit()

# inserting new videos to the db
def insert_vid(conn,video_id,channelTitle, title,description,tags,viewCount,likeCount,commentCount,publishedAt,duration):
    try:
        curr = conn.cursor()
        insert_command = ('''INSERT INTO videos (
            video_id,
            channel_title,
            title,
            "description",
            duration,
            publish_date,
            view_count,
            like_count,
            comment_count,
            tags
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''')
        columns_to_insert = (video_id,channelTitle,title,description,duration,publishedAt,viewCount,likeCount,commentCount,tags)
        curr.execute(insert_command,(columns_to_insert))
    except Exception as e:
        print(e)
    else:
        conn.commit()

def read_all(conn):
    curr = conn.cursor()
    query_command = ('''SELECT * FROM videos;''')
    # curr.execute(query_command)
    # data = curr.fetchall()
    data = pd.read_sql_query(query_command,conn)
    return data
