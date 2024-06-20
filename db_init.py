import pyodbc
import argparse
import db_connector


def table_exists(cursor, table_name):
    """
    Check if a table exists in the database.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        table_name (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}'
    """)
    return cursor.fetchone()[0] == 1


def clear_table(cursor, table_name):
    """
    Clear all data from a table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        table_name (str): The name of the table to clear.
    """
    cursor.execute(f"DELETE FROM {table_name}")


def create_seasons_table(cursor):
    """
    Create the 'season' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE season (
        s_id INT PRIMARY KEY NOT NULL CHECK (s_id >= 1950)
    ); 
    """)


def create_circuit_table(cursor):
    """
    Create the 'circuit' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE circuit (
        c_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
        c_name NVARCHAR(255) NOT NULL,
        c_location NVARCHAR(50),
        c_country NVARCHAR(50)
    );
    """)


def create_races_table(cursor):
    """
    Create the 'race' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE race (
        r_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
        r_name NVARCHAR(255),
        r_date DATE NOT NULL,
        r_time NVARCHAR(50) NULL,
        r_fk_c_id INT FOREIGN KEY REFERENCES circuit(c_id) ON DELETE CASCADE ON UPDATE CASCADE,
    );
    """)


def create_drivers_table(cursor):
    """
    Create the 'driver' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE driver (
        d_id NVARCHAR(50) PRIMARY KEY NOT NULL,
        d_number INT CHECK (d_number BETWEEN 0 AND 99),
        d_code NVARCHAR(10),
        d_firstname NVARCHAR(255),
        d_surname NVARCHAR(255),
        d_dob DATE,
        d_nationality NVARCHAR(255)
    );
    """)


def create_constructors_table(cursor):
    """
    Create the 'constructor' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE constructor (
        con_id NVARCHAR(50) PRIMARY KEY NOT NULL,
        con_name NVARCHAR(255),
        con_nationality NVARCHAR(255)
    );
    """)


def create_results_table(cursor):
    """
    Create the 'result' table.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    cursor.execute("""
    CREATE TABLE result (
        res_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
        res_grid INT,
        res_position INT,
        res_points DECIMAL(5, 2) DEFAULT 0,
        res_laps INT,
        res_time NVARCHAR(50),
        res_status NVARCHAR(255),
        res_fk_r_id INT FOREIGN KEY REFERENCES race(r_id) ON DELETE CASCADE ON UPDATE CASCADE,
        res_fk_d_id NVARCHAR(50) FOREIGN KEY REFERENCES driver(d_id) ON DELETE CASCADE ON UPDATE CASCADE,
        res_fk_con_id NVARCHAR(50) FOREIGN KEY REFERENCES constructor(con_id) ON DELETE CASCADE ON UPDATE CASCADE,
    );
    """)


def initialize_tables(conn):
    """
    Initialize the tables in the database.

    Parameters:
        conn (pyodbc.Connection): The connection object to the MSSQL database.
    """
    cursor = conn.cursor()

    tables = {
        'Season': create_seasons_table,
        'Circuit': create_circuit_table,
        'Race': create_races_table,
        'Driver': create_drivers_table,
        'Constructor': create_constructors_table,
        'Result': create_results_table,
    }

    # Check if each table exists, clear it if it does, or create it if it doesn't
    for table_name, create_function in tables.items():
        if table_exists(cursor, table_name):
            print(f"The table '{table_name}' already exists. Emptying it's contents.")
            clear_table(cursor, table_name)
        else:
            print(f"The table '{table_name}' does not yet exist. Creating the table.")
            create_function(cursor)

    conn.commit()  # Commit the transaction
    cursor.close()  # Close the cursor


def main():
    """
    The main function to parse arguments and initialize the database tables.
    """
    parser = argparse.ArgumentParser(description="Initialises the MSSQL database if not already done.")
    parser.add_argument('--server', required=True, help='The MSSQL server name.')
    parser.add_argument('--database', required=True, help='The MSSQL database name.')
    parser.add_argument('--username', required=True, help='The MSSQL username.')
    parser.add_argument('--password', required=True, help='The MSSQL password.')

    args = parser.parse_args()

    try:
        conn = db_connector.connect_db(args.server, args.database, args.username, args.password)  # Connect to the database
        initialize_tables(conn)  # Initialize the tables
        conn.close()  # Close the connection
        print("Tables were initialized successfully.")
    except Exception as e:
        print(f"Error: {e}")  # Print any errors that occur


if __name__ == "__main__":
    main()
