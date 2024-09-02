import scrapetube
import pandas as pd 
import re
from googleapiclient.discovery import build 
import time
import datetime 


def generateCSV(api_key,username):

    #Using api key and youtube api, build youtube
    youtube = build("youtube",'v3', developerKey = api_key)
    #Make a request that grabs statistics on a given channel
    request = youtube.channels().list(part = "statistics", forUsername = username)
    response = request.execute()

    #Of the statistics, grab the userID 
    items = response["items"]
    items_dict = items[0]
    user_ID = items_dict["id"]

    #Given a user_ID, scrape all video data and put into a dataframe 
    videos = scrapetube.get_channel(user_ID)
    videos_DF = pd.DataFrame(videos)


    #FUNCTIONS
    def remakeLengthText(df):
        lengthTextModified = []
        for rowVal in df["lengthText"]:
            lengthTextModified.append(rowVal["accessibility"]["accessibilityData"]["label"])
    
        df["lengthText"] = pd.DataFrame(lengthTextModified)
        return df
    
    def remakeSingleDict(df,columnName):
        columnModified = []
        pattern = r"\{'simpleText':\s*'(.*?)'\}"
        for rowVal in df[columnName]:
            rowVal = str(rowVal)
            match = re.search(pattern,rowVal)
            
            if match:
                value = match.group(1)
                columnModified.append(value)
        
        #columnModified = pd.DataFrame(columnModified)
        #return pd.DataFrame(columnModified)
        
        df[columnName] = pd.DataFrame(columnModified)
        return df

    def remakeTitle(df):
        titleColumnModified = []
        for rowVal in df["title"]:
            titleColumnModified.append(rowVal["runs"][0]["text"])
            
        df["title"] = pd.DataFrame(titleColumnModified)
        return df
    
    def remakeThumbnail(df):
        thumbnailColumnModified = []
        for rowVal in df["thumbnail"]:
            thumbnailColumnModified.append(rowVal["thumbnails"][0]["url"])
            
        df["thumbnail"] = pd.DataFrame(thumbnailColumnModified)
        return df
    

    def fixViews(df):
        df_column_mod = df["viewCountText"].apply(lambda x: x.replace(" views", ""))
        df_column_mod = df_column_mod.apply(lambda x: x.replace(",",""))
        df_column_mod.astype("int64")
        df["viewCountText"] = df_column_mod
        return df
    
    def get_video_publish_dates(youtube, df):
        publish_dates = []

        for i in range(0, df.shape[0], 50):
            batch_ids = df["videoId"][i:i+50]
            request = youtube.videos().list(
                part='snippet',
                id=','.join(batch_ids)
            )
            response = request.execute()

            for item in response.get('items', []):
                video_id = item['id']
                publish_date = item['snippet']['publishedAt']
                publish_dates.append(publish_date)

            time.sleep(1)  # To avoid hitting rate limits

        df["publishedAt"] = pd.DataFrame(publish_dates)
        return df  
    
    def lengthTextModified(df):
        lengthTextSeconds = []
        lengthTextTime = []
        for rowVal in df["lengthText"]:
            pattern = r'(?:(\d+)\s*hours?,?\s*)?(?:(\d+)\s*minutes?,?\s*)?(?:(\d+)\s*seconds?)'
            match = re.search(pattern, rowVal)

            if match:
                rowHours = match.group(1) or 0
                rowMinutes = match.group(2) or 0
                rowSeconds = match.group(3) or 0 

            if rowHours != 0:
                rowHours = int(rowHours)
            if rowMinutes != 0:
                rowMinutes = int(rowMinutes)
            if rowSeconds != 0:
                rowSeconds = int(rowSeconds)

            time = datetime.time(rowHours,rowMinutes,rowSeconds)
            totalSeconds = (rowHours*3600) + (rowMinutes*60) + (rowSeconds)
            lengthTextTime.append(time)
            lengthTextSeconds.append(totalSeconds)
        
        df["lengthTextTime"] = pd.DataFrame(lengthTextTime)
        df["lengthTextSeconds"] = pd.DataFrame(lengthTextSeconds)
        return df 

    videos_DF = remakeLengthText(videos_DF)
    videos_DF = remakeSingleDict(videos_DF,"publishedTimeText")
    videos_DF = remakeSingleDict(videos_DF,"viewCountText")

    videos_DF = videos_DF[videos_DF["publishedTimeText"].notna()]
    videos_DF = videos_DF[videos_DF["lengthText"].notna()]
    videos_DF = videos_DF[videos_DF["viewCountText"].notna()]

    videos_DF = remakeTitle(videos_DF)
    videos_DF = remakeThumbnail(videos_DF)
    videos_DF = fixViews(videos_DF)
    videos_DF = get_video_publish_dates(youtube,videos_DF)
    videos_DF = lengthTextModified(videos_DF)

    videos_DF_mod = videos_DF_mod.drop(["publishedTimeText","lengthText","descriptionSnippet","navigationEndpoint","ownerBadges","trackingParams","showActionMenu","shortViewCountText","menu","thumbnailOverlays","richThumbnail","badges","topStandaloneBadge"], axis = 1)

    videos_DF_mod.to_csv(str(username) + ".csv",index = True)