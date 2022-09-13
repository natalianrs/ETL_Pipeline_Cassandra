# Building a data pipeline to load data in Cassandra 

## Project Structure 🛠
1. Creating a data model
2. Extracting data from different sources 
3. Loading data into Cassandra 

## Repository Structure  📂
      ├── Event_Data             # .csv data files   
      ├── Event_Datafile.csv     # contains transformed data    
      ├── cql_queries.py         # contains cql queries with create/drop/insert/delete statements    
      ├── etl.py                 # script to extract data from sources, transform and load into cassandra tables    
      └── Test.ipynb             # jupyter notebook contains routines to validate the data insertion quality     

## Data Modeling 💾

## Data Pipeline ⚙

### How to run 
1. Execute `$ python create_tables.py`;
2. Execute `$ python etl.py`;

🐱‍👤
