import pandas as pd
import requests
import time
import json
import datetime as dt
from airflow import DAG
from airflow.decorators import task, task_group
import pymongo

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

with DAG(dag_id='ETL', start_date=dt.datetime(2022, 1, 1),
         schedule_interval=None,
         description="ETL project with airflow tool",
         tags=['artists', 'last.fm', 'discogs.com']) as dag:
    @task
    def extract_info_from_artist(name: str):
        # extract for all artists' informations from last fm and store as a dict
        artist_contents = {}
        url = 'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=' + name + '&api_key=' + LASTFM_API_KEY + '&format=json'
        artist_info = requests.get(url).json()
        artist_contents.update({str(name): artist_info['artist']['bio']['content']})
        print('Search information for artist {} ...'.format(str(name)))

        return artist_contents


    @task
    def extract_titles_from_artist(name: str):
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + name + '&{?type=artist}&token=' + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        # with id get artist's releases
        url = 'https://api.discogs.com/artists/' + str(id) + '/releases?token=' + DISCOGS_API_KEY

        releases = requests.get(url).json()

        print('Found releases from discogs.com for artist ' + str(name) + ' with Discogs ID: ' + str(id))

        return {name: releases['releases']}


    @task
    def extract_info_for_titles(releases: dict):
        # store the releases/tracks info in a list
        releases_info = []

        key = list(releases.keys())
        artist = str(key[0])
        for index in range(len(releases[artist])):
            url = releases[artist][index]['resource_url']
            params = {'token': DISCOGS_API_KEY}
            source = requests.get(url, params=params).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                if 'formats' in source.keys():
                    releases_info.append({'Title': source['title'],
                                          'Collaborations': releases[artist][index]['artist'],
                                          'Year': source['year'],
                                          'Format': source['formats'][0]['name'],
                                          'Discogs Price': source['lowest_price']})
                else:
                    releases_info.append({'Title': source['title'],
                                          'Collaborations': releases[artist][index]['artist'],
                                          'Year': source['year'],
                                          'Format': None,
                                          'Discogs Price': source['lowest_price']})
                print('Found informations from discogs.com for title {}'.format(source['title']))
                # sleep 4 secs to don't miss requests
                time.sleep(4)

        # return artist's tracks for transform stage
        return releases_info


    @task
    def extract_playcounts_from_titles_by_artist(releases: dict):
        # initialize list for playcounts for each title
        playcounts = []
        # find listeners from lastfm for each release title
        key = list(releases.keys())
        artist = str(key[0])
        for index in range(len(releases[artist])):
            title = releases[artist][index]['title']
            url = 'https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=' + LASTFM_API_KEY + '&artist=' + \
                  artist + '&track=' + title + '&format=json'

            try:
                source = requests.get(url).json()
                if 'track' in source.keys():
                    playcounts.append({'Title': source['track']['name'],
                                       'Lastfm Playcount': source['track']['playcount']})
                    print('Found playcount from last.fm for title {}'.format(title))
                else:
                    print('Not found playcount from last.fm for title {}'.format(title))
            except:
                print('Not found playcount from last.fm for title {}'.format(title))
                continue

        return playcounts


    @task
    def clean_the_artist_content(content: dict):
        content_df = pd.DataFrame(content.values(), columns=['Content'], index=content.keys())

        # remove new line commands, html tags and "", ''
        content_df['Content'] = content_df['Content'].replace(r'\r+|\n+|\t+', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r'<[^<>]*>', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r'"', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r"'", '', regex=True)
        print('Clean the informations text')

        return content_df.to_dict(orient='index')


    @task
    def remove_wrong_values(releases: dict):
        df = pd.DataFrame(releases)
        # find and remove the rows/titles where there are no selling prices in discogs.com
        df = df[df['Discogs Price'].notna()]
        print('Remove releases where there no selling price in discogs.com')
        # keep only the rows has positive value of year
        df = df[df['Year'] > 0]
        print('Remove releases where have wrong year value in discogs.com')

        return df.to_dict(orient='records')


    @task
    def merge_titles_data(releases: dict, playcounts: dict):
        releases_df = pd.DataFrame(releases)
        playcounts_df = pd.DataFrame(playcounts)

        df = pd.merge(releases_df, playcounts_df, on='Title')
        print('Merge releases and playcounts data')

        return df.to_dict(orient='records')


    @task
    def sort_titles_by_price(releases: dict):
        df = pd.DataFrame(releases)
        # sort descending
        df = df.sort_values(['Discogs Price', 'Title'], ascending=False)
        print('Sort titles by highest value')

        return df.to_dict(orient='records')


    @task
    def drop_duplicates_titles(releases: dict):
        df = pd.DataFrame(releases)
        df = df.drop_duplicates(subset=['Title'])
        print('Find and remove the duplicates titles if exist!')
        df = df.set_index('Title')

        return df.to_dict(orient='index')


    @task
    def integrate_data(content: dict, releases: dict) -> None:
        key = list(content.keys())
        artist = str(key[0])
        content.update({artist: {'Description': content[artist]['Content'], 'Releases': releases}})

        # initialize file data to write
        data = {}
        try:
            # check if file exist
            file = open('/home/mentis/airflow/dags/etl_airflow/artists.json', "r")
            # read the artists data that was already writen in file and store as a new data to write
            data.update(json.load(file))
            # add the new artist data
            data.update(content)
        except:
            # If the file is not exist yet, just store the current data to write
            data = content

        # write the new data 
        file = open('/home/mentis/airflow/dags/etl_airflow/artists.json', "w")
        json.dump(data, file)
        return content


    @task_group(group_id='extract_transform_stage')
    def transform(artist_names: list):
        for name in artist_names:
            releases = extract_titles_from_artist(name)
            integrate_data(clean_the_artist_content(extract_info_from_artist(name)),
                           drop_duplicates_titles(sort_titles_by_price(
                               merge_titles_data(remove_wrong_values(extract_info_for_titles(releases)),
                                                 extract_playcounts_from_titles_by_artist(releases)))))


    @task
    def load_to_database():
        client = pymongo.MongoClient(
            "mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
        db = client["mydatabase"]
        artists = db['artists']

        with open('/home/mentis/airflow/dags/etl_airflow/artists.json', 'r') as artist_file:
            data = json.load(artist_file)

            for artist in list(data.keys()):
                artists.insert_one({'Artist': str(artist),
                                    'Description': data[str(artist)]['Description'],
                                    'Releases': data[str(artist)]['Releases']
                                    })
                print('Artist {} insert to DataBase!'.format(str(artist)))


    @task_group(group_id='load_stage')
    def load():
        load_to_database()


    df = pd.read_csv(
        '/home/mentis/airflow/dags/etl_airflow/spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())

    transform(artist_names[:4]) >> load()
