import json

"""""
filename = '/home/mentis/airflow/dags/etl_airflow/artists.json'

content = {'ababa': {'Description': 'content','Releases': 'releases'}}

data = {}
try:
    file =  open(filename, "r")
    data.update(json.load(file))
    data.update(content)
    #file =  open(filename, "w")
    print('Try')
except:
    # If the file is empty, set the data to an empty list
    data = content
    print('Except')
    
file =  open(filename, "w")
json.dump(data, file)

print(type(data))
print(data.keys())
"""""