import re
from database import Database
from database import CursorFromConnectionFromPool

Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')

# Creating the connection time tables
with CursorFromConnectionFromPool() as cursor:
    cursor.execute("DROP TABLE IF EXISTS conn_rush_hour")
    cursor.execute("CREATE TABLE conn_rush_hour ( \
                        pk SERIAL PRIMARY KEY, \
                        line_number VARCHAR, \
                        transport VARCHAR, \
                        frequency FLOAT8, \
                        avg_wait_time FLOAT8 \
                        );")

# Creating list to store line number and transport type
line_number =[]
transport = []

# Fetching line numbers and transport from the existing table, merged ways
with CursorFromConnectionFromPool() as cursor:
    cursor.execute("SELECT DISTINCT line_number, transport \
                    FROM merged_ways \
                    WHERE transport <> 'pedestrian'")
    conn_data = cursor.fetchall()  # Stores the result of the query in the train_data variable
    if conn_data:
        for i in range(len(conn_data)):  # Iterating through all of the conn data
            line_number.append(conn_data[i][0])  # Removing the tuples that comes along with the DB queries
            transport.append(conn_data[i][1])  # Removing the tuples that comes along with the DB queries

# Updating the new tables with line number and transport
for i in range(len(conn_data)):
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("INSERT INTO conn_rush_hour (line_number, transport) \
                        VALUES ('{}', '{}');".format(line_number[i], transport[i]))


# Adding frequency to train in rush hour, units in seconds
train_frq_rh = {
            "a": 600,
            "b": 600,
            "bx": 1200,
            "c": 600,
            "e": 600,
            "f": 600,
            "h": 1200
    }

# Adding frequency to metro in rush hour, units in seconds
metro_frq_rh = {
            "1": 240,
            "2": 240
    }

# Creating empty list for bus_line_numbers
bus_line_number = []

# Fetching bus line numbers
with CursorFromConnectionFromPool() as cursor:
    cursor.execute("SELECT line_number \
                    FROM conn_rush_hour \
                    WHERE transport = 'bus';")
    bus_line_data = cursor.fetchall()
    if bus_line_data:
        for i in range(len(bus_line_data)):
            bus_line_number.append(bus_line_data[i][0]) # Removing the tuples that comes along with the DB queries

# Creating empty dict, that has line numbers as key and frequency as value
bus_frq_rh = {}

# Adding frequency to buses, units in seconds
for i in range(len(bus_line_number)):
    if 'A' in bus_line_number[i]:
        bus_frq_rh[bus_line_number[i]] = 300
    elif 'S' in bus_line_number[i]:
        bus_frq_rh[bus_line_number[i]] = 480
    elif 'E' in bus_line_number[i]:
        bus_frq_rh[bus_line_number[i]] = 900
    elif 'N' in bus_line_number[i]:
        bus_frq_rh[bus_line_number[i]] = 1000000
    else: # yellow buses
        bus_frq_rh[bus_line_number[i]] = 1200

# Updating the database frequency and avg_wait_time values with the frequency dictionaries
for key in train_frq_rh:
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("UPDATE conn_rush_hour \
                        SET frequency = {0}, \
                            avg_wait_time = {0} / 3 \
                        WHERE line_number = '{1}';".format(train_frq_rh[key], key))

for key in metro_frq_rh:
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("UPDATE conn_rush_hour \
                        SET frequency = {0}, \
                            avg_wait_time = {0} / 3 \
                        WHERE line_number = '{1}';".format(metro_frq_rh[key], key))

for key in bus_frq_rh:
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("UPDATE conn_rush_hour \
                        SET frequency = {0}, \
                            avg_wait_time = {0} / 3 \
                        WHERE line_number = '{1}';".format(bus_frq_rh[key], key))
