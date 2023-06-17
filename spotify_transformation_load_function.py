import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_link = row['track']['album']['external_urls']['spotify']
        album_name = row['track']['album']['name']
        rel_date = row['track']['album']['release_date']
        total_tracks = row['track']['album']['total_tracks']
        album = {'album_id':album_id,'album_link':album_link,'album_name':album_name,'rel_date':rel_date,
                 'total_tracks':total_tracks}
        album_list.append(album)
    return album_list
    
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_link = artist['external_urls']['spotify']
                    artists = {'artist_id':artist_id, 'artist_name':artist_name, 'artist_link':artist_link}
                    artist_list.append(artists)
    return artist_list

def songs(data):
    songs_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_link = row['track']['external_urls']['spotify']
        song_name = row['track']['name']
        song_popularity = row['track']['popularity']
        song_track_no = row['track']['track_number']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        songs = {'song_id':song_id,'song_link':song_link,'song_name':song_name,'song_popularity':song_popularity,
                 'song_track_no':song_track_no, 'song_added':song_added, 'album_id':album_id, 'artist_id':artist_id}
        songs_list.append(songs)
    return songs_list


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-aws'
    Key = 'raw_data/to_process/'
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        songs_df = pd.DataFrame.from_dict(songs_list)
        
        album_df['rel_date'] = pd.to_datetime(album_df['rel_date'])
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added'])
        
        song_key = 'transformed_data/songs_data/song_transformed_' + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        songs_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = 'transformed_data/artists_data/artists_tranformed_' + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
    
