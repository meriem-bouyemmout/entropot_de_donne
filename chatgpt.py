import os
import pandas as pd
import mysql.connector

# Chemin du dossier principal
folder_path = 'Weather Data'

# Parcours récursif des dossiers et fichiers
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            
            # Handling missing values
            missing_values = ["n/a", "nan", "--", ",,", "", "NaN"]
            data = pd.read_csv(file_path, na_values=missing_values, low_memory=False, dtype=str)
            
            # Fill missing values with mode
            for column in ['TMIN', 'TMAX', 'TAVG', 'PRCP', 'STATION', 'NAME', 'LATITUDE', 'LONGITUDE']:
                if column in data.columns:
                    mode_value = data[column].mode()
                    if not mode_value.empty:
                        data[column].fillna(mode_value[0], inplace=True)

            # Drop rows with any remaining missing values
            data.dropna(inplace=True)
            
            # Convert numeric columns to appropriate types
            numeric_columns = ['TMIN', 'TMAX', 'TAVG', 'PRCP']
            for column in numeric_columns:
                if column in data.columns:
                    data[column] = pd.to_numeric(data[column], errors='coerce')

            # Connect to the database
            db_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="shema"
            )
            cursor = db_connection.cursor(buffered=True)

            for _, row in data.iterrows():
                # Insert station data if not exists
                cursor.execute("""
                    INSERT IGNORE INTO station (station, name, latitude, longitude, elevation) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (row['STATION'], row['NAME'], row['LATITUDE'], row['LONGITUDE'], row['ELEVATION']))

                # Get station ID
                cursor.execute("SELECT id FROM station WHERE station = %s", (row['STATION'],))
                station_id = cursor.fetchone()
                if station_id:
                    station_id = station_id[0]
                else:
                    # If the station was just inserted, get its ID using LAST_INSERT_ID()
                    cursor.execute("SELECT LAST_INSERT_ID()")
                    station_id = cursor.fetchone()[0]

                # Insert date data if not exists
                cursor.execute("""
                    INSERT IGNORE INTO date (date) 
                    VALUES (%s)
                """, (row['DATE'],))

                # Get date ID
                cursor.execute("SELECT id FROM date WHERE date = %s", (row['DATE'],))
                date_id = cursor.fetchone()
                if date_id:
                    date_id = date_id[0]
                else:
                    # If the date was just inserted, get its ID using LAST_INSERT_ID()
                    cursor.execute("SELECT LAST_INSERT_ID()")
                    date_id = cursor.fetchone()[0]

                # Insert weather data
                cursor.execute("""
                    INSERT INTO fact_temperature (id_date, id_station, PRCP, TAVG, TMAX, TMIN) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (date_id, station_id, row['PRCP'], row['TAVG'], row['TMAX'], row['TMIN']))

            db_connection.commit()
            cursor.close()
            db_connection.close()
