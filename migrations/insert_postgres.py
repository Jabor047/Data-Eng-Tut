import pandas as pd
import mysql.connector as mysql
import psycopg2
import os, sys
from io import StringIO
from config import p_pass


def connect(dwhName):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(user="postgres",
                                password=p_pass,
                                host="127.0.0.1",
                                # port=,
                                database=dwhName)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)

    print("Connection successful")
    return conn


def DBConnect(dwhName=None):
    """

    Parameters
    ----------
    dwhName :
        Default value = None)

    Returns
    -------

    """
    conn = mysql.connect(host='localhost', user='root',
                         database=dwhName, buffered=True)
    cur = conn.cursor()
    print('Successfully Connected!')
    return conn, cur


def insert_postgres(dwhName: str, table_name: str, df: pd.DataFrame) -> None:
    connection = psycopg2.connect(user="postgres",
                                password=p_pass,
                                host="127.0.0.1",
                                # port=,
                                database=dwhName)

    cursor = connection.cursor()
    connection.autocommit = True
    
    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (station_id, flow_99, flow_max,
                                flow_median, flow_total, n_obs)
            VALUES(%s, %s, %s, %s, %s, %s)"""
        data = (int(row[0]), float(row[1]), float(row[2]),
                float(row[3]), float(row[4]), float(row[5]),)

        try:
            # Execute the SQL command
            cursor.execute(sqlQuery, data)
            # Commit your changes in the database
            connection.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            connection.rollback()
            print("Error: ", e)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


def insert_to_summary(dwhName: str, table_name: str, df) -> None:
    """

    Parameters
    ----------
    dwhName :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:

    Returns
    -------

    """
    conn, cur = DBConnect(dwhName)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (station_id, flow_99, flow_max,
                                flow_median, flow_total, n_obs)
             VALUES(%s, %s, %s, %s, %s, %s)"""
        data = (int(row[0]), float(row[1]), float(row[2]),
                float(row[3]), float(row[4]), float(row[5]),)

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return


def copy_from_file(conn, df, table):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, header=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, null='', sep=',')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)


def copy_from_stringio(conn: str, df: pd.DataFrame, table: str) -> None:
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer)
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()


if __name__ == "__main__":
    dwhName = 'Sensor_DW'
    table_name = 'dimStation'
    conn, cur = DBConnect(dwhName)
    connection = connect(dwhName.lower())

    # summary_df = pd.read_sql("SELECT * FROM dimStationSummary", conn)\
    #     .drop('id', axis=1)

    # insert_postgres(dwhName.lower(), 'dimStationSummary', summary_df)

    # copy_from_file(connection, summary_df, 'dimStationSummary'.lower())

    # richard_df = pd.read_sql("Select * From dimRichardStation", conn)\
    #     .drop('id', axis=1)
    # copy_from_file(connection, richard_df, table_name.lower())

    stations_info = pd.read_sql(f"Select * From {table_name}", conn)\
        .drop('id', axis=1)
    copy_from_file(connection, stations_info, table_name.lower())
