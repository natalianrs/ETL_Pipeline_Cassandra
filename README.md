# Building a data pipeline to load data in Cassandra 
## Project Structure 🛠
1. Create a data model by designing tables to answer queries outlined in the project; 
2. Process data by Extracting data from source and creating a streamlined csv data file  
3. Loading data into Cassandra

## Repository Structure  📂
      ├── Event_Data             # .csv data files   
      ├── Event_Datafile.csv     # contains transformed data    
      ├── cql_queries.py         # contains cql queries with create/drop/insert/delete statements    
      ├── etl.py                 # script to extract data from sources, transform and load into cassandra tables    
      └── Test.ipynb             # jupyter notebook contains routines to validate the data insertion quality     

## Modeling and Querying data in Cassandra  💾
**Queries** <br>
The app team defined different queries to be run in the dataset.  <br>

1. "Which are the artists, song titles, song's length in the music app history that was heard during sessionId=338 and itemInSession=4 ?"
2. "Which are the artist's names, songs (sorted by itemInSession) and users (first and last name) for userid = 10, sessionid = 18 ?"  
3. "Which users (first and last name) in music app history listened to the song 'All Hands Against His Own ?" 

**Data Modeling** <br>
The tables in Cassandra were modeled based on the queries defined for the project. <br>
| Query 1 | Query 2 | Query 3 |
|---|---|---
|**Table Name**: music_history <br> Column 1: sessionID column 2: itemInSession  <br> column 3: artist  <br> column 4: song  <br> column 5: length  <br>  PRIMARY KEY(sessionId, itemInSession)| **Table Name**: user_songs <br> column 1: userId <br> column 2: sessionId <br> column 3: artist <br> column 4: song <br> column 5: firstName <br> column 6:lastName <br> column 7: itemInSession <br> PRIMARY KEY((userId, sessionId), itemInSession) | **Table name**: song_history <br> Column 1: userId <br> Column 2: firstName <br> Column 3: lastName <br> Column 4: song <br> PRIMARY KEY ((userId), song)



## Data Pipeline ⚙
`cql_queries.py` is a module to be imported by ETL.py.   <br>
`etl.py` is the main script that connects to the database, extracts/process/loads data into apache cassandra. <br>
> For each query, the etl.py does: <br>
> - Creates table using: CREATE TABLE IF NOT EXISTS <br>
> - defines primary key, partition key and clustering columns <br>
> - Inserts data into table using INSERT INTO statement  <br>

### How to run <br>
1. Run `$ python etl.py`; <br>
2. Run `test.ipynb` queries to analyze the database 

🐱‍👤
