import pandas as pd
import time
import json
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'


df = pd.read_csv(
    '/home/mentis/airflow/dags/etl_airflow/spotify_artist_data.csv')
names = list(df['Artist Name'].unique())
# add '_' for dag id
dag_names = df['Artist Name'].replace(' ', '_', regex=True)
dag_names = list(dag_names)


with DAG(dag_id='ETL', start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
         schedule_interval=None,
         description="ETL project with airflow tool",
         catchup=False, tags=['artist', 'lastfm', 'discogs']) as dag:

    @task
    def start():
        print('Start Pipeline...!')

    @task
    def extract_artist_names():
        df = pd.read_csv(
            '/home/mentis/airflow/dags/etl_airflow/spotify_artist_data.csv')
        names = list(df['Artist Name'].unique())
        return json.dumps({name: None for name in names[:2]})

    @task(multiple_outputs=True)
    def extract_info_from_artist(name: str):
        # extract for all artists' informations from last fm and store as a dict
        artist_contents = {}
        url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + str(name) + (
                '&api_key=') + str(LASTFM_API_KEY) + ('&format=json')
        artist_info = requests.get(url).json()
        artist_contents.update({str(name): artist_info['artist']['bio']['content']})
        print('Search information for artist {} ...'.format(str(name)))

        # create dataframe by artist names as index
        contents_df = pd.DataFrame(artist_contents.values(), columns=['Content'], index=artist_contents.keys())
        # return artist info as a dict object for transform stage
        return contents_df.to_dict(orient='index')


    @task()
    def extract_titles_from_artist(name: str):
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + str(name) + (
            '&{?type=artist}&token=') + DISCOGS_API_KEY
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
            time.sleep(5)

        print('Find tracks from artist ' + str(name) + ' with Discogs ID: ' + str(id))
        return json.dumps({'Title': title_info, 'Collaborations': colab_info, 'Year': year_info,
                           'Format': format_info, 'Discogs Price': price_info})
    
    #names = extract_artist_names()
    for name in names[:2]:

        [extract_info_from_artist(name), extract_titles_from_artist(name)] >> start()
        
        
    

"""""
extract_info_from_artist = PythonOperator(
    task_id='extract_info_from_Drake',
    python_callable=extract_info_from_artist,
    op_args= str(names[0]),
    dag=dag
)

extract_titles_from_artist1 = PythonOperator(
    task_id='extract_titles_from_{}'.format(str(dag_names[0])),
    python_callable=extract_titles_from_artist,
    op_args=[names[0]],
    dag=dag
)

extract_titles_from_artist2 = PythonOperator(
    task_id='extract_titles_from_{}'.format(str(dag_names[1])),
    python_callable=extract_titles_from_artist,
    op_args=[names[1]],
    dag=dag
)
"""""
"""""
names, names_copy = extract_artist_names()
task_start >> [extract_titles_from_artist1, extract_titles_from_artist2]
"""""
