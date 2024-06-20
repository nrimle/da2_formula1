import pyodbc


def connect_db(server, database, username, password):
    """
    Establish a connection to the MSSQL database.

    Parameters:
        server (str): The name of the MSSQL server.
        database (str): The name of the MSSQL database.
        username (str): The username for the MSSQL database.
        password (str): The password for the MSSQL database.

    Returns:
        pyodbc.Connection: The connection object to the MSSQL database.
    """
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', server=server, database=database, uid=username,
                          pwd=password,
                          trusted_connection='yes')
    return conn
