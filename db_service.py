from matplotlib.pyplot import table
import psycopg2 as ps

# Connect to your postgres DB
def connect_to_db(host,port,username,password):
    try:
        conn = ps.connect(host=host,user=username,password=password,port=port)
    except Exception as e:
        raise e
    else:
        print('Connected Successfully!')
    return conn

# create table
def create_table(conn):
    try:
        curr = conn.cursor()
        create_table_command = ('''CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(100) PRIMARY KEY,
            title TEXT NOT NULL,
            "description" TEXT NOT NULL,
            duration VARCHAR NOT NULL,
            publish_date DATE NOT NULL DEFAULT currENT_DATE,
            view_count INTEGER NOT NULL,
            like_count INTEGER NOT NULL,
            comment_count INTEGER NOT NULL,
            liveBroadcastContent VARCHAR NOT NULL)''')
        curr.execute(create_table_command)
    except Exception as e:
        raise e
    else:
        conn.commit()

# check to see if video exists
def check_if_video_exists(conn,video_id):
    try:
        curr = conn.cursor()
        query = ('''SELECT video_id FROM videos WHERE video_id = %s''')
        curr.execute(query,(video_id,))
    except Exception as e:
        raise e
    else:
        return(curr.fetchone() is not None)

# update video details on the db
def update_vid(conn,video_id,title, description,duration,publish_date,view_count,like_count,comment_count,liveBroadcastContent):
    try:
        curr = conn.cursor()
        query = ('''UPDATE videos
                SET title = %s,
                "description" = %s,
                duration = %s,
                publish_date = %s,
                view_count = %s,
                like_count = %s,
                comment_count = %s,
                liveBroadcastContent = %s
                WHERE video_id = %s''')
        columns_to_update = (title, description,duration,publish_date,view_count,like_count,comment_count,liveBroadcastContent,video_id)
        curr.execute(query,(columns_to_update))
    except Exception as e:
        raise e
    else:
        conn.commit()

# inserting new videos to the db
def insert_vid(conn,video_id,title, description,duration,publish_date,view_count,like_count,comment_count,liveBroadcastContent):
    try:
        curr = conn.cursor()
        query = ('''INSERT INTO videos (video_id,
                title,
                "description",
                duration,
                publish_date,
                view_count,
                like_count,
                comment_count,
                liveBroadcastContent)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);''')
        columns_to_insert = (video_id,title, description,duration,publish_date,view_count,like_count,comment_count,liveBroadcastContent)
        curr.execute(query,(columns_to_insert))
    except Exception as e:
        raise e
    else:
        conn.commit()
