import pandas as pd
import time
import json
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

# [START instantiate_dag]

with DAG(dag_id="ETL", start_date=pendulum.datetime(2021, 1, 1,  tz="UTC"), schedule_interval=None,
         description="ETL project with airflow tool",
         catchup=False, tags=['lastfm', 'discogs']) as dag:
    @task()
    def extract_artist_names():
        df = pd.read_csv('~/python/anaconda3/envs/condenv/lib/python3.6/site-packages/airflow/example_dags/etl_airflow/spotify_artist_data.csv')
        # return artist names
        names = df['Artist Name'].unique()
        print(names)
        print(type(names))
        return list(names)

    @task()
    def extract_info_from_all_artists(artists_names: list):
        artist_contents = {}
        print(type(artists_names))
        # extract for all artists' informations from last fm and store as a dict
        for name in artists_names:
            url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + str(name) + (
                '&api_key=') + str(LASTFM_API_KEY) + ('&format=json')
            artist_info = requests.get(url).json()
            artist_contents.update({str(name): artist_info['artist']['bio']['content']})
            print('Search infrmation for artist {} ...'.format(str(name)))

        # return artist info as a dataframe for transform stage
        return pd.DataFrame(artist_contents.values(), columns=['Content'], index=artist_contents.keys())

    @task()
    def extract_titles_from_artist(name: str):
        # get the artist id from artist name
        url = ('https://api.discogs.com/database/search?q=') + name + ('&{?type=artist}&token=') + str(DISCOGS_API_KEY)
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search titles from artist ' + name + '...')

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()
        releases_df = pd.json_normalize(releases['releases'])

        # store the tracks info in a list
        tracks_info = []
        for index, url in enumerate(releases_df['resource_url'].values):
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                # print(str(index) + ': '+ str(source['title'])+ ' '+ str(source['lowest_price']))
                if 'formats' in source.keys():
                    tracks_info.append((source['title'], releases_df['artist'].iloc[index], source['year'],
                                        source['formats'][0]['name'], source['lowest_price']))
                else:
                    tracks_info.append(
                        (source['title'], releases_df['artist'].iloc[index], source['year'], None,
                         source['lowest_price']))
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(3)

        print('Find tracks from artist ' + name + ' with Discogs ID: ' + str(id))

        # return artist's tracks as a dataframe for transform stage
        return pd.DataFrame(tracks_info, columns=['Title', 'Collaborations', 'Year', 'Format', 'Discogs Price'])


    artist_names = extract_artist_names()
    content_df = extract_info_from_all_artists(artist_names[:2])
    releases_df = extract_titles_from_artist(artist_names[0])