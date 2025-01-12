from googleapiclient.discovery import build

import pymongo

import psycopg2

import pandas as pd

import streamlit as st

from datetime import timedelta

#API CONNECT

def Api_connect():
    Api_Id='Use Your Youtube API Key'

    api_service_name='youtube'
    api_version='v3'

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#Get Channel Information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics,status",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(Channel_name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                Channel_status=i['status']['privacyStatus'])
    return data 

    

#get video id

def get_videos_id(channel_id):
    video_Ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    PlayList_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=PlayList_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_Ids

#GET VIDEO INFORMATION

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnails=item['snippet']['thumbnails']['default']['url'],
                    Descritption=item['snippet'].get('description'),
                    Published_Data=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Dislikes=item['statistics'].get('dislikeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Defnition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data
    
#Get Comment Details

def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                          Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_data.append(data)

    except:
        pass
    return comment_data

#GET Playlist Details

def get_playlist_details(channel_id):
        playlist_data=[]
        next_page_token=None

        while True:
                request=youtube.playlists().list(
                        channelId=channel_id,
                        part='contentDetails,snippet',
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                channel_Id=item['snippet']['channelId'],
                                channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                video_Count=item['contentDetails']['itemCount'])
                        playlist_data.append(data)
                
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return playlist_data

#Upload to MongoDB

client=pymongo.MongoClient('mongodb+srv://aquib__javed:password@cluster0.ov7zr9u.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db=client['Youtube_Data_Harvesting']

#Inserting channel details in Mongodb

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_id(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    table1=db['channel_details']
    table1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,'video_information':vi_details,'comment_information':com_details})
    
    return 'Upload Completed Successfully'

#Table creation for channels,playlist,videos and comments

#Table creation for Channels

def channels_table(channel_name_s):

    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()

    create_query='''create table if not exists channels(channel_name varchar(100),
                                                            channel_Id varchar(80) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            total_videos int,
                                                            channel_description text,
                                                            playlist_id varchar(80),
                                                            channel_status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()

    singe_channel_details=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({'channel_information.Channel_name':channel_name_s},{'_id':0}):
        singe_channel_details.append(ch_data['channel_information'])
    df_single_channel_details=pd.DataFrame(singe_channel_details)
 

    for index,row in df_single_channel_details.iterrows():
        insert_query='''insert into channels(channel_name,
                                            channel_Id,
                                            subscribers,
                                            views,
                                            total_videos,
                                            channel_description,
                                            playlist_id,
                                            channel_status)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'],
                row['Channel_status'])
        
        
        try:

            cursor.execute(insert_query,values)
            mydb.commit()

        except:

            news=f'Your Provided Channel Name {channel_name_s} already exists'

            return news
            
        

#PLYLIST TABLE CREATION
def playlists_table(channel_name_s):
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key, 
                                                        Title varchar(100),
                                                        channel_Id varchar(100),
                                                        channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        video_Count int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    singe_playlist_details=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({'channel_information.Channel_name':channel_name_s},{'_id':0}):
        singe_playlist_details.append(ch_data['playlist_information'])
    df_single_playlist_details=pd.DataFrame(singe_playlist_details[0])

    for index,row in df_single_playlist_details.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            channel_Id,
                                            channel_Name,
                                            PublishedAt,
                                            video_Count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Title'],
                row['channel_Id'],
                row['channel_Name'],
                row['PublishedAt'],
                row['video_Count']
                )
        
        
        cursor.execute(insert_query,values)
        mydb.commit()

#Video Table Creation


def video_table(channel_name_s):
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnails varchar(150),
                                                    Descritption text,
                                                    Published_Data timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Dislikes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Defnition varchar(10),
                                                    Caption_Status varchar(150)
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    singe_video_details=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({'channel_information.Channel_name':channel_name_s},{'_id':0}):
        singe_video_details.append(ch_data['video_information'])
    df_single_video_details=pd.DataFrame(singe_video_details[0])


    for index,row in df_single_video_details.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnails,
                                            Descritption,
                                            Published_Data,
                                            Duration,
                                            Views,
                                            Likes,
                                            Dislikes,
                                            Comments,
                                            Favorite_Count,
                                            Defnition,
                                            Caption_Status
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnails'],
                row['Descritption'],
                row['Published_Data'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Dislikes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Defnition'],
                row['Caption_Status']
                )
        
        
        cursor.execute(insert_query,values)
        mydb.commit()
    
#Uploading comnt info into posql


def comment_table(channel_name_s):
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()


    create_query='''create table if not exists Comments(Comment_Id Varchar(100) primary key,
                                                    Video_Id varchar(50),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published timestamp
                                                    )'''
    cursor.execute(create_query)
    mydb.commit() 

    singe_comments_details=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({'channel_information.Channel_name':channel_name_s},{'_id':0}):
        singe_comments_details.append(ch_data['comment_information'])
    df_singe_comments_details=pd.DataFrame(singe_comments_details[0])


    for index,row in df_singe_comments_details.iterrows():
        insert_query='''insert into Comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published
                                            )
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )
        cursor.execute(insert_query,values)
        mydb.commit()

#TABLE CREATION FUNCTION FOR SQL

def tables(channel_name_s):

    news = channels_table(channel_name_s)
    if news:
        return news
    else:  
        playlists_table(channel_name_s)
        video_table(channel_name_s)
        comment_table(channel_name_s)
        return 'Tables created successfully'

#Table view in streamlit

def show_channels_tables():
    ch_list=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df

def show_playlists_tables():
    pl_list=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for pl_data in table1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_tables():
    vi_list=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for vi_data in table1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_tables():
    cmd_list=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for cmd_data in table1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(cmd_data['comment_information'])):
            cmd_list.append(cmd_data['comment_information'][i])
    df3=st.dataframe(cmd_list)

    return df3

#Streamlit Part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Integration')
    st.caption('Data Mangment using MongoDB and SQL')
    st.header('About Project')
    st.markdown('- Building a simple UI with Streamlit')
    st.markdown('- Retriving data from Youtube API')
    st.markdown('- Migrating it to the SQL data warehouse')
    st.markdown('- Quering data warehouse with SQL')
    st.markdown('- Displaying data in **Streamlit** app')

channel_id=st.text_input('Enter the Channel ID')

if st.button('Collect and store Data'):
    ch_ids=[]
    db=client['Youtube_Data_Harvesting']
    table1=db['channel_details']
    for ch_data in table1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
    
    if channel_id in ch_ids:
        st.success('Channel details of the given Channel Id already exists')
    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channels=[]
db=client['Youtube_Data_Harvesting']
table1=db['channel_details']
for ch_data in table1.find({},{'_id':0,'channel_information':1}):
    all_channels.append(ch_data['channel_information']['Channel_name'])

unique_channel=st.selectbox('Select the Channel',all_channels)

if st.button('Migrate to SQL'):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio('SELECT THE TABLE FOR VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table=='CHANNELS':
    show_channels_tables()

elif show_table=='PLAYLISTS':
    show_playlists_tables()

elif show_table=='VIDEOS':
    show_videos_tables()

elif show_table=='COMMENTS':
    show_comments_tables()

#SQL Connection

mydb=psycopg2.connect(host='localhost',
                    user='postgres',
                    password='',
                    database='youtube_data',
                    port='5432')
cursor=mydb.cursor()



question=st.selectbox('Select Your Question',('1. All the Videos and Channel Names',
                                            '2. Channels with most number of Videos',
                                            '3. 10 most viewed Videos',
                                            '4. Comments in each Videos',
                                            '5. Videos with highest likes',
                                            '6. Likes and Dislikes of all Videos',
                                            '7. Views of each Channels',
                                            '8. Videos published in the year of 2024',
                                            '9. Average duration of videos in each Channel',
                                            '10. Videos with highest number of comments'))

def execute_query(query, columns):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        if results:  # Check if results are found
            df = pd.DataFrame(results, columns=columns)
            st.write(df)
        else:
            st.warning("No results found for your query.")
    except psycopg2.Error as e:
        st.success("Please migrate channel information to SQL to use Queries")

#QUESTION:1

if question=='1. All the Videos and Channel Names':
    query1='''select title as videos,channel_name as channelname from videos'''
    execute_query(query1, ['Video Title', 'Channel Name'])


#QUESTION:2

elif question=='2. Channels with most number of Videos':
    query2='''select channel_name as channelname,total_videos as no_videos from channels
            order by total_videos desc'''
    execute_query(query2, ['Channel Name', 'No of Videos'])


#QUESTION:3

elif question=='3. 10 most viewed Videos':
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
            where views is not null order by views desc limit 10'''
    execute_query(query3, ['Views', 'Channel Name', 'Video Title'])


#QUESTION:4

elif question=='4. Comments in each Videos':
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    execute_query(query4, ['No of Comments', 'Video Title'])

#QUESTION:5

elif question=='5. Videos with highest likes':
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
            from videos where likes is not null order by likes desc'''
    execute_query(query5, ['Video Title', 'Channel Name', 'Likes Count'])


#QUESTION:6

elif question=='6. Likes and Dislikes of all Videos':
    query6='''select likes as like_count, dislikes as dislike_count,title as video_title from videos'''
    execute_query(query6, ['Likes Count', 'Dislike Count', 'Video Title'])


#QUESTION:7

elif question=='7. Views of each Channels':
    query7='''select channel_name as channelname,views as total_views from channels'''
    execute_query(query7, ['Channel Name', 'Total Views'])


#QUESTION:8

elif question=='8. Videos published in the year of 2024':
    query8='''select title as video_title,published_data as videorelease,channel_name as channelname from videos
            where extract(year from published_data)=2024'''
    execute_query(query8, ['Video Title', 'Published Date', 'Channel Name'])

#QUESTION:9

elif question=='9. Average duration of videos in each Channel':
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    try:
        cursor.execute(query9)
        results = cursor.fetchall()
    
    # Convert average duration (in seconds) to minutes and seconds
        formatted_results = []
        for row in results:
            channel_name = row[0]
            average_duration = row[1]

            if isinstance(average_duration, timedelta):
                average_seconds = average_duration.total_seconds()
            else:
                average_seconds = average_duration
            
            # Calculate minutes and seconds
            minutes = int(average_seconds // 60)
            seconds = int(average_seconds % 60)
            
            # Format the average duration as "X min Y sec"
            formatted_duration = f"{minutes} min {seconds} sec"
            
            formatted_results.append((channel_name, formatted_duration))
        
        # Create a DataFrame to display the results
        df9 = pd.DataFrame(formatted_results, columns=['Channel Name', 'Average Duration'])
        st.write(df9)

    except psycopg2.Error as e:
        st.success("Please migrate channel information to SQL to use Queries")

#QUESTION:10

elif question=='10. Videos with highest number of comments':
    query10='''select title as video_title,channel_name as channelname,comments as comments from videos
            where comments is not null order by comments desc'''
    execute_query(query10, ['Video Title', 'Channel Name', 'Comments'])













