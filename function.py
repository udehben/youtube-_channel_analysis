import requests
import time
from tqdm import tqdm


def get_vidoes_per_page(url,params,df):
    resp = requests.get(url,params=params).json()
    # Wait 1 sec to be sure all request have been made
    time.sleep(1)

    nextPageToken = resp.get('nextPageToken',None)
    for video in tqdm(resp["items"]):
        if video["id"]["kind"] == "youtube#video":
            video_id = video["id"]['videoId']
            publish_date = video["snippet"]["publishedAt"].split('T')[0]
            title = video["snippet"]["title"]
            description = video["snippet"]["description"]
            channelTitle = video["snippet"]["channelTitle"]
            liveBroadcastContent = video["snippet"]["liveBroadcastContent"]

            #get single vidoe data
            duration,viewCount,likeCount,commentCount = get_single_vidoe_detais(video_id,params['key'])


            # Save data in pandas dataframe
            df = df.append({
                'video_id':video_id,
                'channelTitle':channelTitle,
                'publish_date':publish_date,
                'title':title,
                'description':description,
                'liveBroadcastContent':liveBroadcastContent,
                'duration':duration,
                'viewCount':viewCount,
                'likeCount':likeCount,
                'commentCount':commentCount
            }, ignore_index=True)
    return (nextPageToken,df)

def get_vidoes(df,API_KEY,channelId):
    url = 'https://www.googleapis.com/youtube/v3/search'
    param = {'key': API_KEY,
             'channelId': channelId,
             'part':'snippet',
             'maxResults':50,
             'order':'date'}
    nextPageToken, df = get_vidoes_per_page(url,param,df)
    idx = 0
    while (nextPageToken is not None and idx < 10):
        param['pageToken'] = nextPageToken
        nextPageToken, df = get_vidoes_per_page(url,param,df)
        idx +=1
    return(df)

def get_single_vidoe_detais(vid_id,API_KEY):
    url = 'https://www.googleapis.com/youtube/v3/videos'
    part = ['contentDetails','statistics']
    for p in part:
        param = {'key': API_KEY,
                'id': vid_id,
                'part':p
                }
        resp = requests.get(url,params=param).json()
        if p == 'contentDetails':
            duration = resp['items'][0]['contentDetails']['duration'].strip('PT')
        else:
            viewCount = resp['items'][0]['statistics']['viewCount']
            likeCount = resp['items'][0]['statistics']['likeCount']
            commentCount = resp['items'][0]['statistics']['commentCount']
    return(duration,viewCount,likeCount,commentCount)