import requests
import pyodbc
import argparse
import time
import db_connector
import json


def fetch_data(url):
    """
    Fetch data from the Ergast API.

    Parameters:
        url (str): The URL to fetch data from.

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    time.sleep(0.5)  # Sleep for 500 milliseconds to avoid rate limit
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


def season_imported(cursor, season):
    """
    Check if the season has already been imported.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        season (int): The season year to check.

    Returns:
        bool: True if the season has been imported, False otherwise.
    """
    cursor.execute("SELECT COUNT(*) FROM season WHERE s_id = ?", (season,))
    return cursor.fetchone()[0] > 0


def mark_season_as_imported(cursor, season):
    """
    Mark a season as imported.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        season (int): The season year to mark as imported.
    """
    cursor.execute("INSERT INTO season (s_id) VALUES (?)", (season,))


def circuit_exists(cursor, circuit):
    """
    Check if a circuit already exists in the database.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        circuit (str): The name of the circuit to check.

    Returns:
        bool: True if the circuit exists, False otherwise.
    """
    cursor.execute("SELECT COUNT(*) FROM circuit WHERE c_name = ?", (circuit,))
    return cursor.fetchone()[0] > 0


def driver_exists(cursor, driver):
    """
    Check if a driver already exists in the database.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        driver (str): The ID of the driver to check.

    Returns:
        bool: True if the driver exists, False otherwise.
    """
    cursor.execute("SELECT COUNT(*) FROM driver WHERE d_id = ?", (driver,))
    return cursor.fetchone()[0] > 0


def constructor_exists(cursor, constructor):
    """
    Check if a constructor already exists in the database.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        constructor (str): The ID of the constructor to check.

    Returns:
        bool: True if the constructor exists, False otherwise.
    """
    cursor.execute("SELECT COUNT(*) FROM constructor WHERE con_id = ?", (constructor,))
    return cursor.fetchone()[0] > 0


def get_circuit_id(cursor, circuit_name):
    """
    Retrieve the circuit ID for a given circuit name.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        circuit_name (str): The name of the circuit.

    Returns:
        int: The ID of the circuit, or None if not found.
    """
    cursor.execute("SELECT c_id FROM circuit WHERE c_name = ?", (circuit_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None


def get_race_id(cursor, race_date):
    """
    Retrieve the race ID for a given race date.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        race_date (str): The date of the race.

    Returns:
        int: The ID of the race, or None if not found.
    """
    cursor.execute("SELECT r_id FROM race WHERE r_date = ?", (race_date,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None


def import_circuit(cursor, circuit):
    """
    Import a single circuit into the MSSQL database if it doesn't already exist.

    Parameters:
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
        circuit (dict): The circuit data to import.
    """
    c_name = circuit['circuitName']
    c_location = circuit['Location']['locality']
    c_country = circuit['Location']['country']

    if not circuit_exists(cursor, c_name):
        cursor.execute(
            "INSERT INTO circuit (c_name, c_location, c_country) VALUES (?, ?, ?)",
            c_name, c_location, c_country
        )


def import_races(season, cursor):
    """
    Import race data into the MSSQL database.

    Parameters:
        season (int): The season year to import.
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    data = fetch_data(f"http://ergast.com/api/f1/{season}.json")
    races = data['MRData']['RaceTable']['Races']

    for race in races:
        r_name = race['raceName']
        r_date = race['date']
        r_time = race.get('time', None)
        circuit = race['Circuit']

        import_circuit(cursor, circuit)
        r_fk_c_id = get_circuit_id(cursor, circuit['circuitName'])

        cursor.execute(
            "INSERT INTO race (r_name, r_date, r_time, r_fk_c_id) VALUES (?, ?, ?, ?)",
            r_name, r_date, r_time, r_fk_c_id
        )


def import_drivers(season, cursor):
    """
    Import driver data into the MSSQL database.

    Parameters:
        season (int): The season year to import.
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    data = fetch_data(f"http://ergast.com/api/f1/{season}/drivers.json")
    drivers = data['MRData']['DriverTable']['Drivers']

    for driver in drivers:
        d_id = driver['driverId']
        d_number = driver.get('permanentNumber', None)
        d_code = driver.get('code', None)
        d_firstname = driver['givenName']
        d_surname = driver['familyName']
        d_dob = driver['dateOfBirth']
        d_nationality = driver['nationality']

        if not driver_exists(cursor, d_id):
            cursor.execute(
                "INSERT INTO driver (d_id, d_number, d_code, d_firstname, d_surname, d_dob, d_nationality) VALUES (?, ?, ?, ?, ?, ?, ?)",
                d_id, d_number, d_code, d_firstname, d_surname, d_dob, d_nationality
            )


def import_constructors(season, cursor):
    """
    Import constructor data into the MSSQL database.

    Parameters:
        season (int): The season year to import.
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    data = fetch_data(f"http://ergast.com/api/f1/{season}/constructors.json")
    constructors = data['MRData']['ConstructorTable']['Constructors']

    for constructor in constructors:
        con_id = constructor['constructorId']
        con_name = constructor['name']
        con_nationality = constructor['nationality']

        if not constructor_exists(cursor, con_id):
            cursor.execute(
                "INSERT INTO constructor (con_id, con_name, con_nationality) VALUES (?, ?, ?)",
                con_id, con_name, con_nationality
            )


def import_results(season, cursor):
    """
    Import race results data into the MSSQL database.

    Parameters:
        season (int): The season year to import.
        cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
    """
    data = fetch_data(f"http://ergast.com/api/f1/{season}/results.json?limit=1000")
    races = data['MRData']['RaceTable']['Races']

    for race in races:
        res_fk_r_id = get_race_id(cursor, race['date'])
        results = race['Results']

        for result in results:
            res_grid = result['grid']
            res_position = result['position']
            res_points = result['points']
            res_laps = result['laps']
            res_time = result.get('Time', {}).get('time', None)
            res_status = result['status']
            res_fk_d_id = result['Driver']['driverId']
            res_fk_con_id = result['Constructor']['constructorId']

            cursor.execute(
                "INSERT INTO result (res_grid, res_position, res_points, res_laps, res_time, res_status, res_fk_r_id, res_fk_d_id, res_fk_con_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                res_grid, res_position, res_points, res_laps, res_time, res_status, res_fk_r_id, res_fk_d_id,
                res_fk_con_id
            )


def import_data(season, conn):
    """
    Import all data into the MSSQL database.

    Parameters:
        season (int): The season year to import.
        conn (pyodbc.Connection): The connection object to the MSSQL database.
    """
    cursor = conn.cursor()
    if season_imported(cursor, season):
        print(f"Die Saison {season} wurde bereits importiert.")
        return

    import_races(season, cursor)
    import_drivers(season, cursor)
    import_constructors(season, cursor)
    import_results(season, cursor)
    mark_season_as_imported(cursor, season)
    conn.commit()
    cursor.close()


def main():
    """
    The main function to parse arguments and initiate the data import.
    """
    parser = argparse.ArgumentParser(description="Import Formula 1 data from the Ergast API into an MSSQL database.")
    parser.add_argument('--season', type=int, help='The season year to import.')
    parser.add_argument('--server', required=True, help='The MSSQL server name.')
    parser.add_argument('--database', required=True, help='The MSSQL database name.')
    parser.add_argument('--username', required=True, help='The MSSQL username.')
    parser.add_argument('--password', required=True, help='The MSSQL password.')

    args = parser.parse_args()

    try:
        conn = db_connector.connect_db(args.server, args.database, args.username, args.password)
        import_data(args.season, conn)
        conn.close()
        print(f"Successfully imported data for the season {args.season}.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
