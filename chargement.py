import numpy as np
import pymysql
import pandas as pd
from datetime import datetime


# Fonction pour créer une table avec un schéma donné
def create_table(co_cursor, table_name, table_schema):
    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    sql = "DROP TABLE IF EXISTS " + table_name
    co_cursor.execute(sql)
    sql = "CREATE TABLE " + table_name + "(" + table_schema + ")"
    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    co_cursor.execute(sql)
def get_attribute_type_index(schema, attribute_type):
    row_splt = ","
    ele_splt = " "

    # Create a schema matrix, where the first column stores the attribute's name and the second its type
    temp = schema.split(row_splt)
    # Using list comprehension as shorthand
    schema_matrix = [ele.split(ele_splt) for ele in temp]

    # Get attributes' types list from schema_matrix
    attributes_types = np.array(schema_matrix)[:, 1]

    # Get all the indices of attributes of type 'attribute_type'
    indices = [i for i, e in enumerate(attributes_types) if e.lower() == attribute_type.lower()]
    return indices


# Insert values into a table from a csv file
# Insert values into a table from a csv file
def populate_table(co_cursor, csv_path, table_name, attributes):
    date_indices = get_attribute_type_index(attributes, 'DATE')
    data = pd.read_csv(csv_path, sep=",", encoding='cp1252')

    # Obtenir les noms des colonnes de la table
    table_columns = [col.split()[0] for col in attributes.split(',')]

    for index, row in data.iterrows():
        # Obtenir les colonnes présentes dans le fichier CSV
        csv_columns = list(row.index)

        # Identifier les colonnes manquantes dans le fichier CSV mais présentes dans la table
        missing_columns = [col for col in table_columns if col not in csv_columns]

        # Ajouter les valeurs manquantes comme None
        for col in missing_columns:
            row[col] = None

        attribute_number = (len(attributes.split(',')) * '%s,')[:-1]
        sql = "INSERT INTO " + table_name + " VALUES (" + attribute_number + ")"

        # Convertir les attributs DATE au format MySQL
        if date_indices:
            for date_index in date_indices:
                if not pd.isnull(row[date_index]):
                    row.iloc[date_index] = datetime.strptime(row.iloc[date_index], '%Y-%m-%d').date()
                else:
                    row.iloc[date_index] = None  # Remplacer NaN par None

        # Convertir toutes les valeurs en types compatibles avec MySQL
        row = [val if not pd.isnull(val) else None for val in row]

        co_cursor.execute(sql, tuple(row))


















# Connexion à la base de données
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             database='weather_dataset',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

# Création de la table Weather
create_table(cursor, "Alger",  "STATION varchar(255),NAME varchar(100),LATITUDE float NULL,LONGITUDE float NULL,"
    "ELEVATION float NULL,DATE DATE NULL,ACSH float NULL,ACSH_ATTRIBUTES varchar(25) NULL,PGTM float NULL,PGTM_ATTRIBUTES varchar(25) NULL,PRCP float NULL,PRCP_ATTRIBUTES varchar(25) NULL,"
    "SNOW float NULL,SNOW_ATTRIBUTES varchar(25) NULL,SNWD float NULL,SNWD_ATTRIBUTES varchar(25) NULL,"
    "TAVG float NULL,TAVG_ATTRIBUTES varchar(25) NULL,TMAX float NULL,TMAX_ATTRIBUTES varchar(25) NULL,TMIN float NULL,"
    "TMIN_ATTRIBUTES varchar(25) NULL,WDFG float NULL,WDFG_ATTRIBUTES varchar(25) NULL,"
    "WDFM float NULL,WDFM_ATTRIBUTES varchar(25) NULL,"
    "WSFG float NULL,WSFG_ATTRIBUTES varchar(25) NULL,WSFM float NULL,WSFM_ATTRIBUTES varchar(25) NULL,"
    "WT01 float NULL,WT01_ATTRIBUTES varchar(25) NULL,WT02 float NULL,WT02_ATTRIBUTES varchar(25) NULL,"
    "WT03 float NULL,WT03_ATTRIBUTES varchar(25) NULL,"
    "WT05 float NULL,WT05_ATTRIBUTES varchar(25) NULL,WT07 float NULL,WT07_ATTRIBUTES varchar(25) NULL,"
    "WT08 float NULL,WT08_ATTRIBUTES varchar(25) NULL,WT09 float NULL,WT09_ATTRIBUTES varchar(25) NULL,WT16 float NULL,WT16_ATTRIBUTES varchar(25) NULL,"
    "WT18 float NULL,WT18_ATTRIBUTES varchar(25) NULL")

print("Table alger created")

# Création de la table Moroco
create_table(cursor, "Moroco",  "STATION varchar(255),NAME varchar(100),LATITUDE float NULL,LONGITUDE float NULL,"
    "ELEVATION float NULL,DATE DATE NULL,ACSH float NULL,ACSH_ATTRIBUTES varchar(25) NULL,PGTM float NULL,PGTM_ATTRIBUTES varchar(25) NULL,PRCP float NULL,PRCP_ATTRIBUTES varchar(25) NULL,"
    "SNOW float NULL,SNOW_ATTRIBUTES varchar(25) NULL,SNWD float NULL,SNWD_ATTRIBUTES varchar(25) NULL,"
    "TAVG float NULL,TAVG_ATTRIBUTES varchar(25) NULL,TMAX float NULL,TMAX_ATTRIBUTES varchar(25) NULL,TMIN float NULL,"
    "TMIN_ATTRIBUTES varchar(25) NULL,WDFG float NULL,WDFG_ATTRIBUTES varchar(25) NULL,"
    "WDFM float NULL,WDFM_ATTRIBUTES varchar(25) NULL,"
    "WSFG float NULL,WSFG_ATTRIBUTES varchar(25) NULL,WSFM float NULL,WSFM_ATTRIBUTES varchar(25) NULL,"
    "WT01 float NULL,WT01_ATTRIBUTES varchar(25) NULL,WT02 float NULL,WT02_ATTRIBUTES varchar(25) NULL,"
    "WT03 float NULL,WT03_ATTRIBUTES varchar(25) NULL,"
    "WT05 float NULL,WT05_ATTRIBUTES varchar(25) NULL,WT07 float NULL,WT07_ATTRIBUTES varchar(25) NULL,"
    "WT08 float NULL,WT08_ATTRIBUTES varchar(25) NULL,WT09 float NULL,WT09_ATTRIBUTES varchar(25) NULL,WT16 float NULL,WT16_ATTRIBUTES varchar(25) NULL,"
    "WT18 float NULL,WT18_ATTRIBUTES varchar(25) NULL")

print("Table Moroco created")


# Création de la table Tunisia
create_table(cursor, "Tunisia",  "STATION varchar(255),NAME varchar(100),LATITUDE float NULL,LONGITUDE float NULL,"
    "ELEVATION float NULL,DATE DATE NULL,ACSH float NULL,ACSH_ATTRIBUTES varchar(25) NULL,PGTM float NULL,PGTM_ATTRIBUTES varchar(25) NULL,PRCP float NULL,PRCP_ATTRIBUTES varchar(25) NULL,"
    "SNOW float NULL,SNOW_ATTRIBUTES varchar(25) NULL,SNWD float NULL,SNWD_ATTRIBUTES varchar(25) NULL,"
    "TAVG float NULL,TAVG_ATTRIBUTES varchar(25) NULL,TMAX float NULL,TMAX_ATTRIBUTES varchar(25) NULL,TMIN float NULL,"
    "TMIN_ATTRIBUTES varchar(25) NULL,WDFG float NULL,WDFG_ATTRIBUTES varchar(25) NULL,"
    "WDFM float NULL,WDFM_ATTRIBUTES varchar(25) NULL,"
    "WSFG float NULL,WSFG_ATTRIBUTES varchar(25) NULL,WSFM float NULL,WSFM_ATTRIBUTES varchar(25) NULL,"
    "WT01 float NULL,WT01_ATTRIBUTES varchar(25) NULL,WT02 float NULL,WT02_ATTRIBUTES varchar(25) NULL,"
    "WT03 float NULL,WT03_ATTRIBUTES varchar(25) NULL,"
    "WT05 float NULL,WT05_ATTRIBUTES varchar(25) NULL,WT07 float NULL,WT07_ATTRIBUTES varchar(25) NULL,"
    "WT08 float NULL,WT08_ATTRIBUTES varchar(25) NULL,WT09 float NULL,WT09_ATTRIBUTES varchar(25) NULL,WT16 float NULL,WT16_ATTRIBUTES varchar(25) NULL,"
    "WT18 float NULL,WT18_ATTRIBUTES varchar(25) NULL")

print("Table Tunisia created")






# Peuplement de la table Weather à partir du fichier CSV


populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1920-1929_ALGERIA.csv',  'Alg', 
               'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1930-1939_ALGERIA.csv', 'Alg',  'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1940-1949_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1950-1959_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1960-1969_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1970-1979_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1980-1989_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_1990-1999_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_2000-2009_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_2010-2019_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\\Algeria\\Weather_2020-2022_ALGERIA.csv', 'Alg', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
               'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
               'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
               'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
               'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
               'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
               'WSFM float,WSFM_ATTRIBUTES varchar(25),'
               'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
               'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
               'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
               'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
               'WT18 float,WT18_ATTRIBUTES varchar(25)')

# print("Table alger populated")
populate_table(cursor, 'Weather Data\\Morocco\\Weather_1920-1959_MOROCCO.csv', 'Maroc', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Morocco\\Weather_1960-1989_MOROCCO.csv', 'Maroc', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Morocco\\Weather_1990-2019_MOROCCO.csv', 'Maroc', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Morocco\\Weather_2020-2022_MOROCCO.csv', 'Maroc', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')


print("Table Moroco populated")

# #Tunusia
populate_table(cursor, 'Weather Data\\Tunisia\\Weather_1920-1959_TUNISIA.csv', 'Tunusia', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Tunisia\\Weather_1960-1989_TUNISIA.csv', 'Tunusia', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Tunisia\\Weather_1990-2019_TUNISIA.csv', 'Tunusia', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')

populate_table(cursor, 'Weather Data\\Tunisia\\Weather_2020-2022_TUNISIA.csv', 'Tunusia', 'STATION varchar(255),NAME varchar(100),LATITUDE float,LONGITUDE float,ELEVATION float,'
                'DATE DATE,ACSH float,ACSH_ATTRIBUTES varchar(25),PGTM float,PGTM_ATTRIBUTES varchar(25),PRCP float,PRCP_ATTRIBUTES varchar(25),'
                'SNOW float,SNOW_ATTRIBUTES varchar(25),SNWD float,SNWD_ATTRIBUTES varchar(25),'
                'TAVG float,TAVG_ATTRIBUTES varchar(25),TMAX float,TMAX_ATTRIBUTES varchar(25),'
                'TMIN float,TMIN_ATTRIBUTES varchar(25),WDFG float,WDFG_ATTRIBUTES varchar(25),'
                'WDFM float,WDFM_ATTRIBUTES varchar(25),WSFG float,WSFG_ATTRIBUTES varchar(25),'
                'WSFM float,WSFM_ATTRIBUTES varchar(25),'
                'WT01 float,WT01_ATTRIBUTES varchar(25),WT02 float,WT02_ATTRIBUTES varchar(25),'
                'WT03 float,WT03_ATTRIBUTES varchar(25),WT05 float,WT05_ATTRIBUTES varchar(25),'
                'WT07 float,WT07_ATTRIBUTES varchar(25),WT08 float,WT08_ATTRIBUTES varchar(25),'
                'WT09 float,WT09_ATTRIBUTES varchar(25),WT16 float,WT16_ATTRIBUTES varchar(25),'
                'WT18 float,WT18_ATTRIBUTES varchar(25)')


# print("Table Tunusia populated")

# Fermeture de la connexion à la base de données
cursor.close()
connection.commit()
connection.close()