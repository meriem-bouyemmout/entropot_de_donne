import pymysql
import pandas as pd


def create_date_dim(co_cursor):
    sql = "DROP TABLE IF EXISTS Date_Dim"
    co_cursor.execute(sql)
    sql = "CREATE TABLE Date_Dim (Date_ID VARCHAR(8), Date VARCHAR(10), Day_Name varchar(10), Day_Name_Abbrev varchar(3), " \
          "Day_Of_Month INT(2), Day_Of_Week INT(1), Day_Of_Year INT(3), holiday_name varchar(35), Is_Holiday varchar(" \
          "5), Is_Weekday varchar(5), Is_Weekend varchar(5), Month_Abbrev varchar(3), Month_End_Flag varchar(5), " \
          "Month_Name varchar(15), Month_Number INT(2), Quarter INT(1), Quarter_Name varchar(6), Quarter_Short_Name " \
          "varchar(2), Same_Day_Previous_Year VARCHAR(10), Same_Day_Previous_Year_ID INT(8), Season varchar(10), " \
          "Week_Begin_Date VARCHAR(10), Week_Begin_Date_ID INT(8), Week_Num_In_Month INT(1), Week_Num_In_Year INT(2), " \
          "Year INT(4), Year_And_Month varchar(7), Year_And_Month_Abbrev varchar(8), Year_And_Quarter varchar(7), " \
          "PRIMARY KEY (Date_ID)) "
    co_cursor.execute(sql)
    print("Table Date_Dim Created")


def create_station_dim(co_cursor):
    sql = "DROP TABLE IF EXISTS station_Dim"
    co_cursor.execute(sql)
    sql = "CREATE TABLE station_Dim (stationID CHAR(5), Name VARCHAR(30), latitude VARCHAR(30),longitude VARCHAR(30) ,"\
        "elevation VARCHAR(30), PRIMARY KEY (stationID)) "
    co_cursor.execute(sql)
    print("Table station_Dim Created")


def create_temperature_fact(co_cursor):
    sql = "DROP TABLE IF EXISTS temperature_Fact"
    co_cursor.execute(sql)
    sql = "CREATE TABLE temperature_Fact (stationID CHAR(20), DateID varchar(" \
          "10), TMIN float, TMAX float, TAVG float, PRCP float, SNOW float, SNWD float, PGTM " \
          "float, PRIMARY KEY (stationID, DateID),"\
          "FOREIGN KEY (DateID) REFERENCES Date_Dim(Date_ID), "\
          "FOREIGN KEY (stationID) REFERENCES station_Dim(stationID)) "
    co_cursor.execute(sql)
    print("Table temperature_Fact Created")



def get_date_id(date_str):
    date_id = date_str.strftime("%Y") + date_str.strftime("%m") + date_str.strftime("%d")
    return date_id


def populate_dim_date(co_cursor, csvpath):
    data = pd.read_csv(csvpath)
    for index, line in data.iterrows():
        sql = "INSERT INTO Date_Dim (Date_ID,Date,Day_Name,Day_Name_Abbrev,Day_Of_Month,Day_Of_Week,Day_Of_Year," \
              "Holiday_Name,Is_Holiday,Is_Weekday,Is_Weekend,Month_Abbrev,Month_End_Flag,Month_Name,Month_Number," \
              "Quarter,Quarter_Name,Quarter_Short_Name,Same_Day_Previous_Year,Same_Day_Previous_Year_ID,Season," \
              "Week_Begin_Date,Week_Begin_Date_ID,Week_Num_In_Month,Week_Num_In_Year,Year,Year_And_Month," \
              "Year_And_Month_Abbrev,Year_And_Quarter)" \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s) "
        holiday_name = line.Holiday_Name
        if pd.isna(line.Holiday_Name):
            holiday_name = None
        co_cursor.execute(sql, (
            line.Date_ID, line.Date, line.Day_Name, line.Day_Name_Abbrev, line.Day_Of_Month, line.Day_Of_Week,
            line.Day_Of_Year, holiday_name, line.Is_Holiday, line.Is_Weekday, line.Is_Weekend, line.Month_Abbrev,
            line.Month_End_Flag, line.Month_Name, line.Month_Number, line.Quarter, line.Quarter_Name,
            line.Quarter_Short_Name, line.Same_Day_Previous_Year, line.Same_Day_Previous_Year_ID, line.Season,
            line.Week_Begin_Date, line.Week_Begin_Date_ID, line.Week_Num_In_Month, line.Week_Num_In_Year, line.Year,
            line.Year_And_Month, line.Year_And_Month_Abbrev, line.Year_And_Quarter))


def populate_tmperature_station(co_cursor):
    co = pymysql.connect(host='localhost',
                         user='root',
                         password='rootroot',
                         database='transactionalDB',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    cursor_sales = co.cursor()
    cursor_sales.execute("SELECT * FROM Alger")
    customersID = []
    for row in cursor_sales:
        # Inserting values into dimension station_dim
        query = "INSERT INTO station_Dim (stationID , Name , latitude ,longitude ,elevation ,"\
                "VALUES (%s, %s, %s, %s, %s)"
        co_cursor.execute(query, (row["STATION"], row["NAME"], row["LATITUDE"], row["LONGTITUDE"], row["ELEVATION"], ))

        # # Inserting values into dimension Customer_Dim
        # if row["CustomerID"] not in customersID:
        #     customersID.append(row["CustomerID"])
        #     cursor_customers = co.cursor()
        #     cursor_customers.execute("SELECT * FROM Customers Where CustomerID = " + row["CustomerID"])
        #     customer = cursor_customers.fetchone()
        #     query = "INSERT INTO Customer_Dim (CustomerID, Name, Country, CountryISO, Region) VALUES (%s, %s, %s, %s, " \
        #             "%s)"
        #     co_cursor.execute(query, (
        #         customer["CustomerID"], customer["CustomerName"], customer["Country"], customer["CountryISO"],
        #         customer["Region"]))

        # Inserting values into fact table Sales_Fact
        query = "INSERT INTO temperature_Fact (stationID , DateID " \
          " TMIN , TMAX , TAVG , PRCP , SNOW , SNWD , PGTM " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        co_cursor.execute(query, (
            row["STATION"], get_date_id(row["Date"]), 
            row["TMIN"], row["TMAX"], row["TAVG"], row["PRCP"], row["SNOW"],))


# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='rootroot',
                             database='DataWarehouse',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()

# Create Data Warehouse schema
create_date_dim(cursor)
create_station_dim(cursor)
create_temperature_fact(cursor)
print("Data Warehouse schema created")

# Populate tables
populate_dim_date(cursor, 'data/Dim_Date_1850-2050.csv')
print("Date dimension populated")
populate_tmperature_station(cursor)
print("Orders dimension populated")
print("Customers dimension populated")
print("Sales Fact populated")

# Close the DB connection
cursor.close()
connection.commit()
connection.close()