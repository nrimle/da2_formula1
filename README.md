# Formula 1 Stats

## Setup
Before running any scripts of this project, it's recommended to start a virtual python env according to the environment: https://docs.python.org/3/library/venv.html
The required dependencies can be installed with `pip install -r requirements.txt`

It's required to have a running MSSQL server that can be reached via a trusted connection.
The database must exist before running any scripts. 
The create_database.sql file can be used to create the database and a user with the necessary permissions. 

## Initialization
To initialize the database, `python db_init.py --server localhost --database FDB2 --username FormulaUser2 --password temp` can be run.
If the script is run, even though the tables already exist, all data will be dropped.

## Data collection
To import the data from the API, `python import.py --server localhost --database FDB2 --username FormulaUser2 --password temp --season 2023` can be run.
Only one season can be imported at a time.

## Views
The SQL commands to create the views are located in the views.sql file.

## Data visualization
To view the visulizations, `jupyter lab` can be run. 
This will open a new tab in your default browser.
Navigate to visualize.ipynb and run the code blocks to visualize the imported data.
