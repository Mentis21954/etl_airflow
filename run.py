import pandas as pd
import time
import json
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

with DAG(dag_id="ETL", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), schedule_interval=None,
         description="ETL project with airflow tool",
         catchup=False, tags=['lastfm', 'discogs']) as dag:
    @task()
    def extract_artist_names():
        df = pd.read_csv(
            '~/python/anaconda3/envs/condenv/lib/python3.6/site-packages/airflow/example_dags/etl_airflow/spotify_artist_data.csv')
        # return artist names
        names = list(df['Artist Name'].unique())
        print(names)
        return {name: None for name in names[:2]}


    @task()
    def extract_info_from_all_artists(artist_content: dict):
        # extract for all artists' informations from last fm and store as a dict
        for name in artist_content.keys():
            url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + str(name) + (
                '&api_key=') + str(LASTFM_API_KEY) + ('&format=json')
            artist_info = requests.get(url).json()
            artist_content.update({str(name): artist_info['artist']['bio']['content']})
            print('Search information for artist {} ...'.format(str(name)))

        # create dataframe by artist names as index
        contents_df = pd.DataFrame(artist_content.values(), columns=['Content'], index=artist_content.keys())
        # return artist info as a dict object for transform stage
        return contents_df.to_dict(orient='index')


    @task()
    def extract_titles_from_artist(artists_names: dict):
        name = list(artists_names.keys())[0]
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + str(name) + ('&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search releases from for discogs.com for artist ' + str(name) + '...')

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()
        releases_df = pd.json_normalize(releases['releases'])

        # store the tracks info in a list
        title_info, colab_info, year_info, format_info, price_info = [], [], [], [], []
        for index, url in enumerate(releases_df['resource_url'].values):
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                # print(str(index) + ': '+ str(source['title'])+ ' '+ str(source['lowest_price']))
                title_info.append(source['title'])
                colab_info.append(releases_df['artist'].iloc[index])
                year_info.append(source['year'])
                price_info.append(source['lowest_price'])
                if 'formats' in source.keys():
                    format_info.append(source['formats'][0]['name'])
                else:
                    format_info.append(None)
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(3)

        print('Find tracks from artist ' + str(name) + ' with Discogs ID: ' + str(id))
        return json.dumps({'Title': title_info, 'Collaborations': colab_info,'Year': year_info,
                           'Format': format_info, 'Discogs Price': price_info})

    artists_names = extract_artist_names()
    contents = extract_info_from_all_artists(artists_names)
    releases = extract_titles_from_artist(artists_names)
