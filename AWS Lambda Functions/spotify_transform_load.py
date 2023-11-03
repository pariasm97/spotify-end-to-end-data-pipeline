import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

def album(data):
    # Create an empty list to store all the resulting dictionaries
    album_list = []
    
    # Iterate through the JSON data to generate a dictionary containing the information for each row.
    for row in data['items']:
        d_album = {'album_id':row['track']['album']['id'], 
                   'album_name':row['track']['album']['name'], 
                   'album_release_date':row['track']['album']['release_date'], 
                   'album_total_tracks':row['track']['album']['total_tracks'], 
                   'album_external_url':row['track']['album']['external_urls']['spotify'], 
                   'album_type':row['track']['album']['type']}
                   
        # Add the resulting dictionary to a list, creating a collection of dictionaries.
        album_list.append(d_album)
    return album_list

def artist(data):
    # Create an empty list to store all the resulting dictionaries
    artist_list = []
    
    # Iterate through the JSON data to generate a dictionary containing the information for each row.
    for row in data['items']:
        for artist in row['track']['artists']:
            d_artist = {'artist_id':artist['id'], 
                        'artist_name':artist['name'], 
                        'artist_external_url':artist['external_urls']['spotify'], 
                        'artist_type':artist['type']}
            
            # Add the resulting dictionary to a list, creating a collection of dictionaries.
            artist_list.append(d_artist)
    return artist_list

def song(data):
    # Create an empty list to store all the resulting dictionaries
    song_list = []
    
    # Iterate through the JSON data to generate a dictionary containing the information for each row.
    for row in data['items']:
        d_song = {
            'song_id':row['track']['id'],
            'song_name':row['track']['name'],
            'song_duration_ms':row['track']['duration_ms'],
            'song_external_url':row['track']['external_urls']['spotify'],
            'song_popularity':row['track']['popularity'],
            'song_added_at':row['added_at'],
            'album_id':row['track']['album']['id'],
            'artist_id':row['track']['album']['artists'][0]['id']}
    
        # Add the resulting dictionary to a list, creating a collection of dictionaries.
        song_list.append(d_song)
    return song_list    

def lambda_handler(event, context):
    # Creating the boto3 client to connect to AWS S3 Bucket
    client = boto3.client('s3')
    Bucket = 'spotify-etl-project-parias'
    Key = 'raw_data/to_process/'
    
    spotify_data = []
    spotify_keys = []
    
    # Looping through all the items in the bucket and extract file key
    for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents'][1:]:
        file_key = file['Key']
        
        # Checking that the file is in JSON format to apply transformations
        if file_key.split('.')[-1] == 'json':
            response = client.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            
            # Appending all the JSON objects and the file keys to separate lists
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    # Looping through the JSON objects and extracting all the information using the define functions
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
    
        # Converting each list into a dataframe
        album_df = pd.DataFrame(album_list)
        artist_df = pd.DataFrame(artist_list)
        song_df = pd.DataFrame(song_list)
        
        # Droping possible duplicates
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        song_df = song_df.drop_duplicates(subset=['song_id'])
        
        # Changing Date columns to proper format
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'], format='mixed', dayfirst=False)
        song_df['song_added_at'] = pd.to_datetime(song_df['song_added_at'], format='mixed', dayfirst=False)
        
        # Feature Engineering: Creating new columns from Date
    
        # Album Dataframe
        album_df['year'] = album_df['album_release_date'].dt.year
        album_df['month'] = album_df['album_release_date'].dt.month
        album_df['day'] = album_df['album_release_date'].dt.day
        album_df['week_day'] = album_df['album_release_date'].dt.weekday
        
        week_day = {
            0: 'Monday',
            1: 'Tuesday',
            2: 'Wednesday',
            3: 'Thursday',
            4: 'Friday',
            5: 'Saturday',
            6: 'Sunday'
        }
        album_df['week_day'] = album_df['week_day'].map(week_day)
        
        # Song Dataframe
        song_df['year'] = song_df['song_added_at'].dt.year
        song_df['month'] = song_df['song_added_at'].dt.month
        song_df['day'] = song_df['song_added_at'].dt.day
        song_df['week_day'] = song_df['song_added_at'].dt.weekday
        song_df['week_day'] = song_df['week_day'].map(week_day)
        
        # Converting each dataframe into a csv file
        # Album
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        
        # Artist
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        
        # Song
        song_key = 'transformed_data/song_data/song_transformed_' + str(datetime.now()) + '.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        
        # Loading transformed information into Amazon S3
        # Album
        client.put_object(
            Bucket=Bucket,
            Key = album_key,
            Body = album_content
            )
        
        # Artist
        client.put_object(
            Bucket=Bucket,
            Key = artist_key,
            Body = artist_content
            )
        
        # Song
        client.put_object(
            Bucket=Bucket,
            Key = song_key,
            Body = song_content
            )
    
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()