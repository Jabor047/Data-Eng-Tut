import pandas as pd
import mysql.connector as mysql
import os

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("Loader").master("local[5]").getOrCreate()


def DBConnect(dwhName=None):
    """

    Parameters
    ----------
    dwhName :
        Default value = None)

    Returns
    -------

    """
    conn = mysql.connect(host=os.getenv('DBT_MYSQL_HOST'), user="root",
                         database=dwhName, buffered=True)
    cur = conn.cursor()
    print("Successfully Connected!")
    return conn, cur


def createDB(dwhName: str) -> None:
    """

    Parameters
    ----------
    dwhName :
        str:
    Returns
    -------

    """
    conn, cur = DBConnect()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {dwhName} CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_unicode_ci;"
    )
    conn.commit()
    cur.close()
    print(f"{dwhName} Successfully created")


def createTables(dwhName: str, schema_name: str) -> None:
    """

    Parameters
    ----------
    dwhName :
        str:

    Returns
    -------

    """
    conn, cur = DBConnect(dwhName)
    sqlFile = schema_name
    fd = open(sqlFile, "r")
    readSqlFile = fd.read()
    fd.close()

    sqlCommands = readSqlFile.split(";")

    for command in sqlCommands:
        try:
            _ = cur.execute(command)
        except Exception as ex:
            print("Command skipped: ", command)
            print(ex)
    conn.commit()
    cur.close()
    print("Table created Successfully!")

    return


def preprocess_df(path: str) -> pd.DataFrame:
    """

    Parameters
    ----------
    df :
        pd.DataFrame:

    Returns
    -------

    """
    df = pd.read_csv(path)

    # df['ID'] = df['ID'].astype(int)
    # df['flow_99'] = df['flow_99'].astype(float)
    # df['flow_max'] = df['flow_max'].astype(float)
    # df['flow_median'] = df['flow_median'].astype(float)
    # df['flow_total'] = df['flow_total'].astype(float)
    # df['n_obs'] = df['n_obs'].astype(float)
    df = df.fillna(0)
    print("Nothing to pre-process")

    return df


# # read all csv data in data/
# def read_stations_data(spark: SparkSession, data_location: str = 'data/*.csv') -> pd.DataFrame:
#     df = spark.read.csv(data_location, header='true')
#     return df


def preprocess_stations(path: str):
    # df = read_stations_data(spark, data_location=path)
    df = pd.read_csv(path)
    df["utc_time_id"] = pd.to_datetime(df["utc_time_id"])
    df = df.fillna(0)

    return df


def preprocess_richards_stations(path: str):
    # df = read_stations_data(spark, data_location=path)
    df = pd.read_csv(path)
    df["timestamp_id"] = pd.to_datetime(df["timestamp_id"])
    df = df.fillna(0)

    return df


def insert_to_dimRichardsStation(dwhName: str, data_path, table_name: str) -> None:
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

    df = preprocess_richards_stations(data_path)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (timestamp_id, flow1, occupancy1, flow2, occupancy2,
                                            flow3, occupancy3, totalflow, weekday, hour, minute, second)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        data = (
            str(row[0]),
            float(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            float(row[5]),
            float(row[6]),
            float(row[7]),
            float(row[8], int(row[9]), int(row[10]), int(row[11])),
        )

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


def insert_to_dimStationSummaryAirflow_table(
    dwhName: str, data_path, table_name: str
) -> None:
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

    df = preprocess_df(data_path)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (station_id, flow_99, flow_max, flow_median, flow_total, n_obs)
             VALUES(%s, %s, %s, %s, %s, %s)"""
        data = (
            int(row[0]),
            float(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            float(row[5]),
        )

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


def insert_to_dimStation_table(dwhName: str, data_path, table_name: str) -> None:
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

    df = preprocess_df(data_path)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (station_id, fwy, dir, district, country, city,
                        statePm, absPm, latitude, longitude, length, type, lanes, name,
                        userId1, userId2, userId3, userId4)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        data = (
            int(row[0]),
            int(row[1]),
            str(row[2]),
            int(row[3]),
            int(row[4]),
            float(row[5]),
            str(row[6]),
            float(row[7]),
            float(row[8]),
            float(row[9]),
            float(row[10]),
            str(row[11]),
            int(row[12]),
            str(row[13]),
            str(row[14]),
            str(row[15]),
            float(row[16]),
            float(row[17]),
        )

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


def insert_to_all_station_table(dwhName: str, data_path: str, table_name: str) -> None:
    conn, cur = DBConnect(dwhName)

    df = preprocess_stations(data_path)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (timestamp_id, station_id, flow1, occupancy1, flow2, occupancy2, flow3,
                        occupancy3, flow4, occupancy4, flow5, occupancy5, flow6, occupancy6, flow7, occupancy7, flow8,
                        occupancy8, flow9, occupancy9, flow0, occupancy10, flow11, occupancy11, flow12, occupancy12)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s)"""
        data = (
            str(row[0]),
            int(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            float(row[5]),
            float(row[6]),
            float(row[7]),
            float(row[8]),
            float(row[9]),
            float(row[10]),
            float(row[11]),
            float(row[12]),
            float(row[13]),
            str(row[14]),
            str(row[15]),
            float(row[16]),
            float(row[17]),
            float(row[18]),
            float(row[19]),
            float(row[20]),
            float(row[21]),
            float(row[22]),
            float(row[23]),
            float(row[24]),
            float(row[25]),
        )

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


def insert_to_tweet_table(dwhName: str, df: pd.DataFrame, table_name: str) -> None:
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

    # df = preprocess_df(df)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (station_id, fwy, dir, district, country, city, statePm, absPm, latitude,
                        longitude, length, type, lanes, name, userId1, userId2, userId3, userId4)
             VALUES(%s, %s, %s, %s, %s, %s)"""
        data = (
            int(row[0]),
            int(row[1]),
            str(row[2]),
            int(row[3]),
            str(row[4]),
            str(row[5]),
            float(row[6]),
            float(row[7]),
            str(row[7]),
            float(row[8]),
            str(row[9]),
            int(row[10]),
            str(row[11]),
            str(row[12]),
            str(row[13]),
            int(row[14]),
            int(row[15]),
        )

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


def db_execute_fetch(
    *args, many=False, tablename="", rdf=True, **kwargs
) -> pd.DataFrame:
    """

    Parameters
    ----------
    *args :

    many :
         (Default value = False)
    tablename :
         (Default value = '')
    rdf :
         (Default value = True)
    **kwargs :


    Returns
    -------

    """
    connection, cursor1 = DBConnect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    # get column names
    field_names = [i[0] for i in cursor1.description]

    # get column values
    res = cursor1.fetchall()

    # get row count and show info
    nrow = cursor1.rowcount
    if tablename:
        print(f"{nrow} recrods fetched from {tablename} table")

    cursor1.close()
    connection.close()

    # return result
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res


if __name__ == "__main__":
    dwhName = "Sensor_DW"
    dimName = "dimAllStations"
    createDB(dwhName=dwhName)
    createTables(dwhName=dwhName, schema_name="/schema/all_stations_schema.sql")

    # df = pd.read_csv('../data_store/I80_stations.csv')

    insert_to_all_station_table(
        dwhName=dwhName,
        data_path="/data/sample_data.csv",
        table_name=dimName,
    )
