from database import CursorFromConnectionFromPool
import formulas


class Bus:
    def __init__(self, acceleration, deceleration, velocity):
        self.acceleration = acceleration
        self.deceleration = deceleration
        self.velocity = velocity
        self.spatial_length = []
        self.id = []

    def __repr__(self):
        return "<Bus>"

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length, pk FROM merged_ways \
                            WHERE connector = 0 AND transport = 'bus'")
            bus_data = cursor.fetchall() # Stores the result of the query in the bus_data variable
            self.spatial_length = [] # Makes sure the list is empty
            self.id = [] # Makes sure the list is empty
            if bus_data:  # None is equivalent to false in boolean expressions
                for i in range(len(bus_data)): # Iterating through all of the bus data
                    self.spatial_length.append(bus_data[i][0]) # Removing the tuples that comes along with the DB queries
                    self.id.append(bus_data[i][1])  # Removing the tuples that comes along with the DB queries
        return None

    def calculate_moving_costs(self):
        # Loading in the spatial lengths
        self.load_length_from_db()

        # Defining sum_time as a list
        sum_time = []

        # Finding the time which it takes to achieve average speed
        acc_time = formulas.time_acc(self.velocity, 0, self.acceleration)

        # Now finding the distance it takes to accelerate
        acc_distance = formulas.dist_acc(self.acceleration, acc_time)

        # Finding the time in which it takes to stop
        dec_time = formulas.time_acc(0, self.velocity, self.deceleration)

        # Now finding the distance it takes to decelerate
        dec_distance = formulas.dist_acc(self.deceleration, dec_time)

        # Finding the remaining distance left
        for i in range(len(self.spatial_length)):
            if self.spatial_length[i] >= (acc_distance + dec_distance):
                distance = self.spatial_length[i] - acc_distance - dec_distance
                # Finding the time that the buss is driving at its average speed
                drive_time = distance / self.velocity
                # Summarizing all the times
                sum_time.append(acc_time + dec_time + drive_time)
            else:
                # Iterating to see what the absolute max speed can be reached and still being able to brake
                sum_time.append(self.__cost_acc_dec_times(self.acceleration, self.deceleration, self.spatial_length[i]))

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
        costs = self.calculate_moving_costs()

        # Running through all of the bus entries
        for i in range(len(self.spatial_length)):
            # Connecting to database
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("UPDATE merged_ways SET time_calc = {} \
                                WHERE pk = {};".format(costs[i], self.id[i]))

    def update_moving_costs(self):
        # Storing the calculated moving costs
        time_calc = self.calculate_moving_costs()

        # Stores all the time_const values and the ids belonging to them
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT time_const, pk FROM merged_ways \
                            WHERE connector = 0 AND transport = 'bus'")
            bus_time_const_costs = cursor.fetchall()  # Stores the result of the query in the bus_data variable
            time_const = [] # Makes sure the list is empty
            time_const_id = [] # Makes sure the list is empty
            if bus_time_const_costs:
                for i in range(len(bus_time_const_costs)): # Iterating through all of the bus data
                    time_const.append(bus_time_const_costs[i][0]) # Removing the tuples that comes along with the DB queries
                    time_const_id.append(bus_time_const_costs[i][1]) # Removing the tuples that comes along with the DB queries

        # Iterates through all non-connector bus ways
        for i in range(len(self.spatial_length)):
            # Iterates through all non-connector bus ways that have a time_const value
            for k in range(len(time_const)):
                # If the time const id matches that of the original bus way and time_const has a value
                # costs are updated according to that. Otherwise it takes the calculated costs
                if self.id[i] == time_const_id[k] and time_const[k] != None:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {} \
                                        WHERE pk = {};".format(time_const[k], time_const_id[k]))
                    # If a match has been found break out of the k-loop
                    break
                else:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {} \
                                        WHERE pk = {};".format(time_calc[i], self.id[i]))

    @staticmethod
    def __load_conn_ways(line_number):
        # List to store conn_ways ids in
        conn_ways = []

        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT pk FROM merged_ways WHERE connector = 1 AND transport = 'bus' AND line_number = %s", [line_number])
            conn_data = cursor.fetchall()  # Stores the result of the query in the bus_data variable
            if conn_data:
                for i in range(len(conn_data)): # Iterating through all of the conn data
                    conn_ways.append(conn_data[i][0]) # Removing the tuples that comes along with the DB queries
            return conn_ways

    def calculate_conn_costs(self, line_number, avg_waiting_time):
        # Getting the ids for the bus ways
        conn_ways = self.__load_conn_ways(line_number)

        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            for i in range(len(conn_ways)):
                cursor.execute('UPDATE merged_ways SET costs = %s WHERE pk = %s', (avg_waiting_time, conn_ways[i]))
