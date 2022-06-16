import requests, pandas as pd
from tqdm import tqdm

def get_video_ids(api_key, channelId):
    video_ids = []
    url = 'https://www.googleapis.com/youtube/v3/search'
    param = {'key': api_key,
             'channelId': channelId,
             'part':'snippet',
             'maxResults':50,
             'order':'date'}
    resp = requests.get(url,param).json()
    for item in resp['items']:
        if item['id']['kind'] == 'youtube#video':
            video_ids.append(item['id']['videoId'])
    num_request = 1
    page_token = resp.get('nextPageToken',None)
    while (page_token is not None and num_request < 10):
        param['pageToken'] = page_token
        resp = requests.get(url,param).json()
        for item in resp['items']:
            if item['id']['kind'] == 'youtube#video':
                video_ids.append(item['id']['videoId'])
        page_token = resp.get('nextPageToken',None)
        num_request += 1
    return video_ids


def get_video_details(api_key, video_ids):
    all_vid_info = []
    for i in tqdm(range(0,len(video_ids),50)):
        url = 'https://www.googleapis.com/youtube/v3/videos'
        part = 'contentDetails','statistics','snippet'
        vid_id = ','.join(video_ids[i:i+50])
        param = {
            'key':api_key,
            'part':part,
            'id':vid_id
        }
        resp = requests.get(url,params=param).json()
        for video in resp['items']:

            vid_info = {}
            vid_info['video_id'] = video["id"]
            vid_info['publishedAt'] = video["snippet"]["publishedAt"]
            vid_info['title'] = video["snippet"]["title"]
            vid_info['description'] = video["snippet"]["description"]
            vid_info['channelTitle'] = video["snippet"]["channelTitle"]
            vid_info['tags'] = video["snippet"]["tags"]
            vid_info['viewCount'] = video["statistics"]["viewCount"]
            vid_info['likeCount'] = video["statistics"]["likeCount"]
            vid_info['commentCount'] = video["statistics"]["commentCount"]
            vid_info['duration'] = video["contentDetails"]["duration"]

            all_vid_info.append(vid_info)
    return pd.DataFrame(all_vid_info)