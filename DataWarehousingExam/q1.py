"""
187157
Saurav Shah
CSE III A
"""

"""
DATABASE 
- (STATE, CITY, MONTH, TEMPERATURES)SET 4

Given Data 

24,16,2,12,2,6,__,16, 25, 25, 30,__,33,19,24,23,13,6,16,__,17,14,__,12

Normalize the data using z-score
normaliazation and then replace missing values with 50th percentile

 

Using OLAP operators, write an SQL query
to find the average temperatures of each state and city during summer
season(months )- with  subtotals for  city and month.

"""

import sqlite3
import math

data_dir = "./data/"

conn = None
SQL = None
try:
    conn = sqlite3.connect(data_dir+"datacube.db")
    SQL = conn.cursor()
except:
    print("Unable to connect to database")
    exit(-1)



#-------------------------PART 1----------------------------------
# Data pre processing

data = [24,16,2,12,2,6,None,16, 25, 25, 30,None,33,19,24,23,13,6,16,None,17,14,None,12]


#Part 1

def get_mean():
    global data
    sum = 0
    n = len(data)
    for x in data:
        if x != None:
            sum += x
    return sum/n


def get_standard_deviation(mean):
    global data
    sum_of_square = 0
    n = len(data)
    for x in data:
        if x != None:
            sum_of_square += (x-mean)**2
    return math.sqrt(sum_of_square/n)



def normalize_using_z_score():
    global data
    """
        X(normalized) = (X - Mean) / (Standard Deviation)
    """
    mean = get_mean()
    std = get_standard_deviation(mean)

    print("Normalize using z score")
    print(f"mean = {mean}\nstandard deviation = {std}")
    print("Input:")
    print(data)
    for i in range(len(data)):
        if data[i] != None:
            data[i] = (data[i] - mean)/std
    print("Output:")
    print(data)


#Part 2
def replace_missing_value_using_50th_percentile():
    global data
    """
        50th percentile is 
    """
    print("Replace missing values with 50th percentile")
    print("Input")
    print(data)

    #sort the data
    temp_arr = []
    for x in data:
        if x != None:
            temp_arr.append(x)

    i=0
    j=0
    while i<len(temp_arr):
        j=i+1

        while j<len(temp_arr):
            if temp_arr[i]>temp_arr[j]:
                temp = temp_arr[i]
                temp_arr[i] = temp_arr[j]
                temp_arr[j] = temp
            j+=1
        i+=1
    print("Sorted data")
    print(temp_arr)
    n=len(temp_arr)
    median = None
    if n%2 == 0:
        median_index = n//2
        median = temp_arr[median_index]
    else:
        median_index = n//2
        median = (temp_arr[median_index] + temp_arr[median_index+1])/2

    print(f"Median = {median}")
    
    for i in range(len(data)):
        if data[i] == None:
            data[i] = median
    print("Output")
    print(data)


def part1():
    print("----------------PART 1---------------------------")
    normalize_using_z_score()
    print()
    replace_missing_value_using_50th_percentile()





#---------------------------------PART 2---------------------------------
#OLAP Operation

"""
Schema
----------

DIMENSION TABLES

Dimension: STATE
---------------------
ID (PK)
NAME

Dimension: CITY
---------------------
ID (PK)
NAME


FACT TABLES

Fact: TEMPERATURE_DATA
-----------------------
SID (FK references ID of STATE)
CID (FK references ID of CITY)

month
temperature


WE use STAR Schema to create the data cube

DATA
---------
* We consider two states (state_1, state_2)
* state_1 has two cities (city_1_state_1,city_2_state_1)
* state_2 has three cities (city_1_state_2,city_2_state_2,city_3_state_2)
* There are three different months (month_1,month_2,month_3)

"""
def create_table(tname,columns):
    """
    Given table name, columns and datatypes,
    execute sql query (sqlite3) to create table
    """
    global SQL
    sql_query = "CREATE TABLE "
    sql_query += tname
    sql_query += "("
    for column in columns:
        sql_query += column["name"] +" ";
        for constraint in column["constraints"]:
            sql_query += constraint
            sql_query += " "
        sql_query += ","
        
    sql_query = sql_query[:-1]
    sql_query += ");"

    print(f"EXECUTING QUERY\n{sql_query}")
    try:
        SQL.execute(sql_query)
        conn.commit()
    except:
        print("TABLE EXISTS")
    

def insert_into_table(tname,columns,values):
    """
    Given table names, table names and values
    execute sql query (sqlite3) to insert the values into table
    """
    global SQL
    sql_query = "INSERT INTO "
    sql_query += tname
    sql_query += "("
    for column in columns:
        sql_query += column
        sql_query += ","
    sql_query = sql_query[:-1]
    sql_query += ")"

    sql_query += " VALUES ("
    for value in values:
        if type(value) == str:
            sql_query += "'"+value+"'"
        else:
            sql_query += str(value)
        sql_query += ","
    sql_query = sql_query[:-1]
    sql_query += ");"

    print(f"EXECUTING QUERY\n{sql_query}")
    try:
        SQL.execute(sql_query)
        conn.commit()
    except:
        print("INSERTION FAILED")


def create_data_cube():
    # Create Dimension tables
    print("CREATING DIMENSION TABLES")
    #Dimension: STATE
    tname = "state"
    columns = \
    [
        {
            "name":"ID",
            "constraints":["TEXT","PRIMARY KEY"]
        },
        {
            "name":"NAME",
            "constraints":["TEXT","NOT NULL"]
        }
    ]
    create_table(tname,columns)

    tname = "city"
    columns = \
    [
        {
            "name":"ID",
            "constraints":["TEXT","PRIMARY KEY"]
        },
        {
            "name":"NAME",
            "constraints":["TEXT","NOT NULL"]
        }
    ]
    create_table(tname,columns)

    # Create Fact tables
    #WE use STAR Schema to create the data cube
    print("CREATING FACT TABLES")
    tname = "temperature_data"
    columns=\
    [   
        {
            "name":"SID",
            "constraints":["TEXT"]
        },
        {
            "name":"CID",
            "constraints":["TEXT"]
        },
        {
            "name":"month",
            "constraints":["TEXT"]
        },
        {
            "name":"temperature",
            "constraints":["INTEGER"]
        },
        {
            "name":"FOREIGN KEY",
            "constraints":["(SID) REFERENCES state(ID)"]
        },
        {
            "name":"FOREIGN KEY",
            "constraints":["(CID) REFERENCES city(ID)"]
        }
    ]
    create_table(tname,columns)
    print()
    print()

    #insert data
    """
    DATA
---------
* We consider two states (state_1, state_2)
* state_1 has two cities (city_1_state_1,city_2_state_1)
* state_2 has three cities (city_1_state_2,city_2_state_2,city_3_state_2)
* There are three different months (month_1,month_2,month_3)
    """
    print("INSERTING DATA")

    #state
    insert_into_table("state",["ID","NAME"],["S1","state_1"])
    insert_into_table("state",["ID","NAME"],["S2","state_2"])

    #city
    insert_into_table("city",["ID","NAME"],["C1","city_1_state_1"])
    insert_into_table("city",["ID","NAME"],["C2","city_2_state_1"])
    insert_into_table("city",["ID","NAME"],["C3","city_1_state_2"])
    insert_into_table("city",["ID","NAME"],["C4","city_2_state_2"])
    insert_into_table("city",["ID","NAME"],["C5","city_3_state_2"])

    #temperature_data
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C1","month1",15])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C1","month2",17])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C1","month3",25])

    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C2","month1",25])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C2","month2",27])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S1","C2","month3",32])

    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C3","month1",35])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C3","month2",37])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C3","month3",45])

    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C4","month1",45])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C4","month2",47])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C4","month3",42])

    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C5","month1",23])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C5","month2",23])
    insert_into_table("temperature_data",["SID","CID","month","temperature"],["S2","C5","month3",12])

    conn.commit()


def olap_operation():
    """
Using OLAP operators, write an SQL query
to find the average temperatures of each state and city during summer
season(months )- with  subtotals for  city and month.
    """

    #average of each state on summer
    #Let summer = "month1","month2"
    sql_query = \
    """
    SELECT 
        sid,name,avg(temperature)
    FROM 
        (select sid,temperature,month from temperature_data) 
    INNER JOIN 
        state 
    ON 
        sid=id 
    WHERE
        month 
    IN
        ("month1","month2")
    GROUP BY
        sid;

    """
    result = SQL.execute(sql_query)
    print("Average temperature of state in summer (month1, month2)")
    print(f"EXECUTING QUERY\n{sql_query}")
    print("------------------------------------")
    print("SID\tNAME\Average Temperature")
    for t in result:
        print(f"{t[0]}\t{t[1]}\t{t[2]}")

    #averate of each city
    """
    SELECT 
        cid,name,avg(temperature)
    FROM 
        (select cid,temperature,month from temperature_data) 
    INNER JOIN 
        city 
    ON 
        cid=id 
    WHERE
        month 
    IN
        ("month1","month2")
    GROUP BY
        cid;

    """
    result = SQL.execute(sql_query)
    print("Average temperature of cities in summer (month1, month2)")
    print(f"EXECUTING QUERY\n{sql_query}")
    print("------------------------------------")
    print("CID\tNAME\Average Temperature")
    for t in result:
        print(f"{t[0]}\t{t[1]}\t{t[2]}")

    
def part2():
    print()
    print("--------------------PART 2------------------")
    create_data_cube()
    olap_operation()

if __name__ == "__main__":
    part1()
    part2()

conn.close()