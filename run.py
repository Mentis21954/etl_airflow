import pandas as pd
import time
import json
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
import pymongo

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'


df = pd.read_csv(
    '/home/mentis/airflow/dags/etl_airflow/spotify_artist_data.csv')
names = list(df['Artist Name'].unique())

with DAG(dag_id='ETL', start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
         schedule_interval=None,
         description="ETL project with airflow tool",
         catchup=False, tags=['artists', 'last.fm', 'discogs.com']) as dag:

    @task
    def start():
        print('Start Workflow...!')

    @task
    def extract_artist_names():
        df = pd.read_csv(
            '/home/mentis/airflow/dags/etl_airflow/spotify_artist_data.csv')
        names = list(df['Artist Name'].unique())
        return {'Artist': names[:2]}
        #return names[:1]
        

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


    @task(multiple_outputs=True)
    def extract_titles_from_artist(name: str):
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + str(name) + (
            '&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search releases from discogs.com for artist {} ...'.format(str(name)))

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
                
                title_info.append(source['title'])
                colab_info.append(releases_df['artist'].iloc[index])
                year_info.append(source['year'])
                price_info.append(source['lowest_price'])
                if 'formats' in source.keys():
                    format_info.append(source['formats'][0]['name'])
                else:
                    format_info.append(None)
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 5 secs to don't miss requests
            time.sleep(5)

        print('Find tracks from artist ' + str(name) + ' with Discogs ID: ' + str(id))
        return {'Title': title_info, 'Collaborations': colab_info, 'Year': year_info,
                           'Format': format_info, 'Discogs Price': price_info}
    

    @task
    def clean_the_artist_content(content: dict):
        content_df = pd.DataFrame.from_dict(content, orient='index')
        # remove new line command and html tags
        content_df['Content'] = content_df['Content'].replace('\n', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r'<[^<>]*>', '', regex=True)
        print('Clean the informations text')

        return content_df.to_dict(orient='index')
    
    @task
    def remove_null_prices(releases: dict):
        df = pd.DataFrame.from_dict(releases)
        print(df.head())
        # find and remove the rows/titles where there are no selling prices in discogs.com
        df = df[df['Discogs Price'].notna()]
        print('Remove tracks where there no selling price in discogs.com')

        return df.to_dict()

    @task
    def drop_duplicates_titles(releases: dict):
        df = pd.DataFrame.from_dict(releases)
        df = df.drop_duplicates(subset=['Title'])
        print('Find and remove the duplicates titles if exist!')
        df = pd.DataFrame(data={'Collaborations': df['Collaborations'].values, 'Year': df['Year'].values,
                                'Format': df['Format'].values,
                                'Discogs Price': df['Discogs Price'].values}, index=(df['Title'].values))
        print(df.head())

        return df.to_dict(orient='index')
        
    @task
    def integrate_data(content: dict, releases: dict) -> None:
        key = list(content.keys())
        artist = str(key[0])
        content.update({artist: {'Description': content[artist]['Content'],'Releases': releases}})

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
        
    
    @task_group(group_id = 'extract_transform_stage')
    def transform(names: list):
        for name in names:
            integrate_data(clean_the_artist_content(extract_info_from_artist(name)), drop_duplicates_titles(remove_null_prices(extract_titles_from_artist(name))))      
          
    @task
    def load_to_database():
        client = pymongo.MongoClient("mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
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


    @task_group(group_id = 'load_stage')
    def load():
        load_to_database()
    
    start() >> transform(names[:3]) >> load()
