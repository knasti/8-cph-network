from database import CursorFromConnectionFromPool

def import_conn_to_db():

    # Creating the connection time tables
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("DROP TABLE IF EXISTS conn_costs")
        cursor.execute("CREATE TABLE conn_costs ( \
                            pk SERIAL PRIMARY KEY, \
                            line_number VARCHAR, \
                            transport VARCHAR, \
                            frequency_rh FLOAT8, \
                            avg_wait_time_rh FLOAT8, \
                            frequency_day FLOAT8, \
                            avg_wait_time_day FLOAT8, \
                            frequency_evening FLOAT8, \
                            avg_wait_time_evening FLOAT8, \
                            frequency_night FLOAT8, \
                            avg_wait_time_night FLOAT8 \
                            );")

    # Creating list to store line number and transport type
    line_number = []
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

    # Updating the new table with line number and transport
    for i in range(len(conn_data)):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("INSERT INTO conn_costs (line_number, transport) \
                            VALUES ('{}', '{}');".format(line_number[i], transport[i]))

    # Adding frequency to trains, units in seconds
    # rush hour

    train_frq_rh = {
                "a": 600,
                "b": 600,
                "bx": 1200,
                "c": 600,
                "e": 600,
                "f": 300,
                "h": 1200
        }

    # day
    train_frq_day = {
                "a": 600,
                "b": 600,
                "bx": 1000000,
                "c": 600,
                "e": 600,
                "f": 300,
                "h": 1200
        }

    # evening
    train_frq_evening = {
                "a": 1200,
                "b": 1200,
                "bx": 1000000,
                "c": 1200,
                "e": 1000000,
                "f": 600,
                "h": 1000000
        }

    # night
    train_frq_night = {
                "a": 1000000,
                "b": 1000000,
                "bx": 1000000,
                "c": 1000000,
                "e": 1000000,
                "f": 1000000,
                "h": 1000000
        }

    # Adding frequency to metro, units in seconds
    # rush hour
    metro_frq_rh = {
                "1": 240,
                "2": 240
        }

    # day
    metro_frq_day = {
                "1": 360,
                "2": 360
        }

    # evening
    metro_frq_evening = {
                "1": 360,
                "2": 360
        }

    # night
    metro_frq_night = {
                "1": 1200,
                "2": 1200
        }

    # Creating empty list for bus_line_numbers
    bus_line_number = []

    # Fetching bus line numbers
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT line_number \
                        FROM conn_costs \
                        WHERE transport = 'bus';")
        bus_line_data = cursor.fetchall()
        if bus_line_data:
            for i in range(len(bus_line_data)):
                bus_line_number.append(bus_line_data[i][0])  # Removing the tuples that comes along with the DB queries

    # Creating empty dicts, that has line numbers as key and frequency as value
    bus_frq_rh = {}
    bus_frq_day = {}
    bus_frq_evening = {}
    bus_frq_night = {}

    # Adding frequency to buses, units in seconds
    for i in range(len(bus_line_number)):
        if 'A' in bus_line_number[i]:
            bus_frq_rh[bus_line_number[i]] = 180
            bus_frq_day[bus_line_number[i]] = 300
            bus_frq_evening[bus_line_number[i]] = 900
            bus_frq_night[bus_line_number[i]] = 1800
        elif 'S' in bus_line_number[i]:
            bus_frq_rh[bus_line_number[i]] = 360
            bus_frq_day[bus_line_number[i]] = 480
            bus_frq_evening[bus_line_number[i]] = 900
            bus_frq_night[bus_line_number[i]] = 3600
        elif 'E' in bus_line_number[i]:
            bus_frq_rh[bus_line_number[i]] = 600
            bus_frq_day[bus_line_number[i]] = 900
            bus_frq_evening[bus_line_number[i]] = 1000000
            bus_frq_night[bus_line_number[i]] = 1000000
        elif 'N' in bus_line_number[i]:
            bus_frq_rh[bus_line_number[i]] = 1000000
            bus_frq_day[bus_line_number[i]] = 1000000
            bus_frq_evening[bus_line_number[i]] = 1000000
            bus_frq_night[bus_line_number[i]] = 7200
        else:  # yellow buses
            bus_frq_rh[bus_line_number[i]] = 1200
            bus_frq_day[bus_line_number[i]] = 1200
            bus_frq_evening[bus_line_number[i]] = 1800
            bus_frq_night[bus_line_number[i]] = 1000000

    # Updating the database frequency and avg_wait_time values with the frequency dictionaries
    for key in train_frq_rh:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_rh = {0}, \
                                avg_wait_time_rh = {0} / 3 \
                            WHERE line_number = '{1}';".format(train_frq_rh[key], key))

    for key in train_frq_day:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_day = {0}, \
                                avg_wait_time_day = {0} / 3 \
                            WHERE line_number = '{1}';".format(train_frq_day[key], key))

    for key in train_frq_evening:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_evening = {0}, \
                                avg_wait_time_evening = {0} / 3 \
                            WHERE line_number = '{1}';".format(train_frq_evening[key], key))

    for key in train_frq_night:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_night = {0}, \
                                avg_wait_time_night = {0} / 3 \
                            WHERE line_number = '{1}';".format(train_frq_night[key], key))

    for key in metro_frq_rh:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_rh = {0}, \
                                avg_wait_time_rh = {0} / 3 \
                            WHERE line_number = '{1}';".format(metro_frq_rh[key], key))

    for key in metro_frq_day:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_day = {0}, \
                                avg_wait_time_day = {0} / 3 \
                            WHERE line_number = '{1}';".format(metro_frq_day[key], key))

    for key in metro_frq_evening:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_evening = {0}, \
                                avg_wait_time_evening = {0} / 3 \
                            WHERE line_number = '{1}';".format(metro_frq_evening[key], key))

    for key in metro_frq_night:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_night = {0}, \
                                avg_wait_time_night = {0} / 3 \
                            WHERE line_number = '{1}';".format(metro_frq_night[key], key))

    for key in bus_frq_rh:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_rh = {0}, \
                                avg_wait_time_rh = {0} / 3 \
                            WHERE line_number = '{1}';".format(bus_frq_rh[key], key))

    for key in bus_frq_day:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_day = {0}, \
                                avg_wait_time_day = {0} / 3 \
                            WHERE line_number = '{1}';".format(bus_frq_day[key], key))

    for key in bus_frq_evening:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_evening = {0}, \
                                avg_wait_time_evening = {0} / 3 \
                            WHERE line_number = '{1}';".format(bus_frq_evening[key], key))

    for key in bus_frq_night:
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE conn_costs \
                            SET frequency_night = {0}, \
                                avg_wait_time_night = {0} / 3 \
                            WHERE line_number = '{1}';".format(bus_frq_night[key], key))

    # Status for the user
    print('Connector table have been created and populated with values')
