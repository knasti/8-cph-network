from database import CursorFromConnectionFromPool
import formulas
from itertools import product


class Bus:
    def __init__(self):
        self.spatial_length = []
        self.id = []
        self.line_number = []

    def __repr__(self):
        return "<Bus>"

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length, pk, line_number FROM merged_ways \
                            WHERE connector = 0 AND transport = 'bus'")
            bus_data = cursor.fetchall() # Stores the result of the query in the bus_data variable
            self.spatial_length = [] # Makes sure the list is empty
            self.id = [] # Makes sure the list is empty
            if bus_data:  # None is equivalent to false in boolean expressions
                for i in range(len(bus_data)): # Iterating through all of the bus data
                    self.spatial_length.append(bus_data[i][0]) # Removing the tuples that comes along with the DB queries
                    self.id.append(bus_data[i][1])  # Removing the tuples that comes along with the DB queries
                    self.line_number.append(bus_data[i][2])  # Removing the tuples that comes along with the DB queries
        return None

    def calculate_moving_costs(self, acceleration, deceleration, velocity):
        # Loading in the spatial lengths
        self.load_length_from_db()

        # Defining sum_time as a list
        sum_time = []

        # Finding the time which it takes to achieve average speed
        acc_time = formulas.time_acc(velocity, 0, acceleration)

        # Now finding the distance it takes to accelerate
        acc_distance = formulas.dist_acc(acceleration, acc_time)

        # Finding the time in which it takes to stop
        dec_time = formulas.time_acc(0, velocity, deceleration)

        # Now finding the distance it takes to decelerate
        dec_distance = formulas.dist_acc(deceleration, dec_time)

        # Finding the remaining distance left
        for i in range(len(self.spatial_length)):
            if self.spatial_length[i] >= (acc_distance + dec_distance):
                distance = self.spatial_length[i] - acc_distance - dec_distance
                # Finding the time that the buss is driving at its average speed
                drive_time = distance / velocity
                # Summarizing all the times
                sum_time.append(acc_time + dec_time + drive_time)
            else:
                # Iterating to see what the absolute max speed can be reached and still being able to brake
                sum_time.append(self.__cost_acc_dec_times(acceleration, deceleration, self.spatial_length[i]))

        return sum_time

    @staticmethod
    def __cost_acc_dec_times(acceleration, deceleration, spatial_length):
        # Iterating through velocities to find the maximum value
        for k in range(1, 10000):
            velocity = k * 0.025

            # Finding the time which it takes to achieve average speed
            acc_time = formulas.time_acc(velocity, 0, acceleration)

            # Now finding the distance it takes to accelerate
            acc_distance = formulas.dist_acc(acceleration, acc_time)

            # Finding the time in which it takes to stop
            dec_time = formulas.time_acc(0, velocity, deceleration)

            # Now finding the distance it takes to decelerate
            dec_distance = formulas.dist_acc(deceleration, dec_time)

            # The braking distance has to be equal to the remaining distance after accelerating
            if dec_distance >= spatial_length - acc_distance - 1 and \
               dec_distance <= spatial_length - acc_distance + 1:
                # Returning the time it takes to get from start to destination
                sum_time = acc_time + dec_time
                return sum_time

    def update_time_calc_costs(self):
        # Storing the moving costs
        costs = self.calculate_moving_costs_2()

        # Running through all of the bus entries
        for i in range(len(self.spatial_length)):
            # Connecting to database
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("UPDATE merged_ways SET time_calc = {} \
                                WHERE pk = {};".format(costs[i], self.id[i]))

    def update_moving_costs(self):
        # Stores all the time_const and time_calc values and the ids belonging to them
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT time_const, time_calc, pk FROM merged_ways \
                            WHERE connector = 0 AND transport = 'bus'")
            bus_time_costs = cursor.fetchall()  # Stores the result of the query in the bus_data variable
            time_const = [] # Makes sure the list is empty
            time_calc = [] # Makes sure the list is empty
            time_id = [] # Makes sure the list is empty
            if bus_time_costs:
                for i in range(len(bus_time_costs)): # Iterating through all of the bus data
                    time_const.append(bus_time_costs[i][0]) # Removing the tuples that comes along with the DB queries
                    time_calc.append(bus_time_costs[i][1])  # Removing the tuples that comes along with the DB queries
                    time_id.append(bus_time_costs[i][2]) # Removing the tuples that comes along with the DB queries

        # Iterates through all non-connector bus ways
        for i in range(len(self.spatial_length)):
            for k in range(len(time_id)):
                # If the time const id matches that of the original bus way and time_const has a value
                # costs are updated according to that. Otherwise it takes the calculated costs
                if self.id[i] == time_id[k] and time_const[k] > 0:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {0}, reverse_costs = {0} \
                                        WHERE pk = {1};".format(time_const[k], time_id[k]))
                    # If a match has been found break out of the k-loop
                    break

                # If time_const is not used, use time_calc
                elif self.id[i] == time_id[k] and time_calc[k] is not None:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {0}, reverse_costs = {0} \
                                        WHERE pk = {1};".format(time_calc[k], time_id[k]))
                    # If a match has been found break out of the k-loop
                    break

    # Updating merged_ways table with connector costs
    @staticmethod
    def update_conn_costs(daytime):
        # daytime = 0, rush hour
        # daytime = 1, day
        # daytime = 2, evening
        # daytime = 3, night
        with CursorFromConnectionFromPool() as cursor:
            if daytime == 0:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_rh, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1 \
                                AND mv.transport = 'bus';")
            if daytime == 1:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_day, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1 \
                                AND mv.transport = 'bus';")
            if daytime == 2:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_evening, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1 \
                                AND mv.transport = 'bus';")
            if daytime == 3:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_night, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1 \
                                AND mv.transport = 'bus';")

    def calculate_moving_costs_2(self):
        self.load_length_from_db()

        # Dict for a bus times, evening times
        a_bus_time = {
                    "1A": 78 * 60,
                    "2A": 56 * 60,
                    "3A": 30 * 60,
                    "4A": 61 * 60,
                    "5A": 54 * 60,
                    "6A": 65 * 60,
                    "8A": 36 * 60,
                    "9A": 61 * 60
            }

        # Dict for s bus times
        s_bus_time = {
            #        "150S": 52 * 60, # Bad representation of city driving
                    "200S": 59 * 60,
                    "250S": 48 * 60,
                    "350S": 92 * 60
            #        "500S": 100 * 60 # Bad representation of city driving
            }

        # Dict for e bus times
        y_bus_time = {
                    "10": 53 * 60,
                    "13": 61 * 60,
                    "37": 34 * 60,
                    "185": 31 * 60
            }

        # Creating an empty list to store bus line numbers in
        bus_line_numbers = []

        # Getting the bus line_numbers, the average velocity will always be calculated from current network
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT DISTINCT line_number FROM velocity.vel_table")
            bus_data = cursor.fetchall()
            if bus_data:
                for i in range(len(bus_data)):
                    bus_line_numbers.append(bus_data[i][0])

        # Dict for bus line lengths
        bus_line_length = {}

        # Finding the length of all bus lines
        for i in range(len(bus_line_numbers)):
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("SELECT spatial_length FROM velocity.vel_table \
                                WHERE line_number = '{}';".format(bus_line_numbers[i]))
                bus_data = cursor.fetchall()
                print(i)
                if bus_data:
                    # Creating a list to store temporarily lengths
                    temp_lengths = []
                    for k in range(len(bus_data)):
                        temp_lengths.append(bus_data[k][0])
                    # Finding the sum of all the links within the bus line
                    length = sum(temp_lengths)
                    bus_line_length[bus_line_numbers[i]] = length

        # Creating a-bus velocity dict
        a_bus_velocity = {}
        a_bus_avg_velocity = 0

        # Creating s-bus velocity dict
        s_bus_velocity = {}
        s_bus_avg_velocity = 0

        # Creating yellow-bus velocity dict
        y_bus_velocity = {}
        y_bus_avg_velocity = 0

        # Calculating average velocity for a-bus lines
        for key in a_bus_time:
            a_bus_velocity[key] = bus_line_length[key] / a_bus_time[key]
            a_bus_avg_velocity = a_bus_avg_velocity + a_bus_velocity[key]

        a_bus_avg_velocity /= len(a_bus_time)

        # Calculating average velocity for s-bus lines
        for key in s_bus_time:
            s_bus_velocity[key] = bus_line_length[key] / s_bus_time[key]
            s_bus_avg_velocity = s_bus_avg_velocity + s_bus_velocity[key]

        s_bus_avg_velocity /= len(s_bus_time)

        # Calculating average velocity for yellow-bus lines
        for key in y_bus_time:
            y_bus_velocity[key] = bus_line_length[key] / y_bus_time[key]
            y_bus_avg_velocity = y_bus_avg_velocity + y_bus_velocity[key]

        y_bus_avg_velocity /= len(y_bus_time)

        print(a_bus_velocity)
        print(a_bus_avg_velocity)

        print(s_bus_velocity)
        print(s_bus_avg_velocity)

        print(y_bus_velocity)
        print(y_bus_avg_velocity)

        # Creating an empty list for the bus times
        bus_time = []

        # Iterating over all bus lengths to determine the cost
        for i in range(len(self.spatial_length)):
            # If A is in line_number, the average velocity for A-buses is used
            if 'A' in self.line_number[i]:
                bus_time.append(self.spatial_length[i] / a_bus_avg_velocity)
            # If S or E is in line_number, the average velocity for S-buses is used
            elif ('S' or 'E') in self.line_number[i]:
                bus_time.append(self.spatial_length[i] / s_bus_avg_velocity)
            # If the above is not true the bus i a yellow or a night bus
            else:
                bus_time.append(self.spatial_length[i] / y_bus_avg_velocity)

        return bus_time
