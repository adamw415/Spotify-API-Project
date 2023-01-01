import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from pathlib import Path
import numpy as np
from datetime import date
from datetime import timedelta

#Authentication
client_id = '____'
secret = '____'
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)



# create empty lists where the results are going to be stored
artist_name = []
track_name = []
popularity = []
track_id = []
release_date = []
duration = []
release_date_precision = []


#extract data from Spotify API
for i in range(0,1000,50):
    track_results = sp.search(q='year:2023', type='track', limit=50,offset=i)
    for i, j in enumerate(track_results['tracks']['items']):
        artist_name.append(j['artists'][0]['name'])
        track_name.append(j['name'])
        track_id.append(j['id'])
        popularity.append(j['popularity'])
        release_date.append(j['album']['release_date'])
        release_date_precision.append(j['album']['release_date_precision'])
        duration.append(j['duration_ms'])


#create df tracks
df_tracks = pd.DataFrame({'artist_name':artist_name,'track_name':track_name, 'track_id':track_id,\
    'duration':duration, 'release_date':release_date, 'release_date_precision':release_date_precision})

#create separate popularity df since the popularity column dynamic and can be joined to the tracks table as needed
df_popularity = pd.DataFrame({'track_id':track_id, 'popularity':popularity})
df_popularity['popular_ind'] = np.where((df_popularity['popularity'] >= 80),1,0)



##Transformations##

df_tracks['duration_seconds'] = df_tracks['duration']/1000
df_tracks['duration_seconds'] = [round(x) for x in df_tracks['duration_seconds']]
df_tracks.drop('duration', axis = 1, inplace = True)

#dudup
df_tracks.drop_duplicates(inplace=True)
df_popularity.drop_duplicates(inplace=True)

#filter for release dates from yesterday to append to existing table
df_tracks = df_tracks[df_tracks['release_date'] == str(date.today() - timedelta(days = 1))]

#add bucket and indicator columns for analysis
df_tracks['duration_category'] = np.select([(df_tracks.duration_seconds < 150),(df_tracks.duration_seconds >= 150) & (df_tracks.duration_seconds <= 240), \
    (df_tracks.duration_seconds> 240)], ['short', 'medium','long'])

#reorder columns
df_tracks.loc[:,['track_id','track_name','artist_name','duration_seconds','duration_category','release_date','release_date_precision']] 



##LOAD##

#load tables into BigQuery
df_tracks.to_gbq(destination_table = 'SPOTIFY_API.TRACKS', project_id='lithe-optics-373318',if_exists='append')
df_popularity.to_gbq(destination_table = 'SPOTIFY_API.POPULARITY', project_id='lithe-optics-373318',if_exists='replace')



