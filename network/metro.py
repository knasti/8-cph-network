from database import CursorFromConnectionFromPool
import formulas


class Metro:
    def __init__(self):
        self.spatial_length = []
        self.id = []

    def __repr__(self):
        return "<Metro>"

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length, pk FROM merged_ways \
                            WHERE connector = 0 AND transport = 'metro'")
            metro_data = cursor.fetchall() # Stores the result of the query in the metro_data variable
            self.spatial_length = [] # Makes sure the list is empty
            self.id = [] # Makes sure the list is empty
            if metro_data:  # None is equivalent to false in boolean expressions
                for i in range(len(metro_data)): # Iterating through all of the metro data
                    self.spatial_length.append(metro_data[i][0]) # Removing the tuples that comes along with the DB queries
                    self.id.append(metro_data[i][1])  # Removing the tuples that comes along with the DB queries
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
        for item in self.spatial_length:
            if item >= acc_distance + dec_distance:
                distance = item - acc_distance - dec_distance
                # Finding the time that the metros is driving at its average speed
                drive_time = distance / velocity
                # Summarizing all the times
                sum_time.append(acc_time + dec_time + drive_time)
            else:
                # Iterating to see what the absolute max speed can be reached and still being able to brake
                sum_time.append(self.__cost_acc_dec_times(acceleration, deceleration, item))

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

    def update_time_calc_costs(self, acceleration, deceleration, velocity):
        # Storing the moving costs
        costs = self.calculate_moving_costs(acceleration, deceleration, velocity)

        # Running through all of the metro entries
        for i in range(len(self.spatial_length)):
            # Connecting to database
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("UPDATE merged_ways SET time_calc = {} \
                                WHERE pk = {};".format(costs[i], self.id[i]))

    def update_moving_costs(self):
        # Stores all the time_const and time_calc values and the ids belonging to them
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT time_const, time_calc, pk FROM merged_ways \
                            WHERE connector = 0 AND transport = 'metro'")
            train_time_const_costs = cursor.fetchall()  # Stores the result of the query in the train_data variable
            time_const = [] # Makes sure the list is empty
            time_calc = []  # Makes sure the list is empty
            time_id = [] # Makes sure the list is empty
            if train_time_const_costs:
                for i in range(len(train_time_const_costs)): # Iterating through all of the train data
                    time_const.append(train_time_const_costs[i][0]) # Removing the tuples that comes along with the DB queries
                    time_calc.append(train_time_const_costs[i][1])  # Removing the tuples that comes along with the DB queries
                    time_id.append(train_time_const_costs[i][2]) # Removing the tuples that comes along with the DB queries

        # Iterates through all non-connector metro ways
        for i in range(len(self.spatial_length)):
            # Iterates through all non-connector metro ways that have a time_const value
            for k in range(len(time_const)):
                # If the time const id matches that of the original metro way and time_const has a value
                # costs are updated according to that. Otherwise it takes the calculated costs
                if self.id[i] == time_id[k] and time_const[k] > 0:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {0}, reverse_costs = {0} \
                                        WHERE pk = {1};".format(time_const[k], time_id[k]))
                    # If a match has been found break out of the k-loop
                    break
                elif self.id[i] == time_id[k] and time_calc[k] is not None:
                    with CursorFromConnectionFromPool() as cursor:
                        cursor.execute("UPDATE merged_ways SET costs = {0}, reverse_costs = {0} \
                                        WHERE pk = {1};".format(time_calc[k], time_id[i]))
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
                                AND mv.connector = 1\
                                AND mv.transport = 'metro';")
            elif daytime == 1:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_day, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1\
                                AND mv.transport = 'metro';")
            elif daytime == 2:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_evening, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1\
                                AND mv.transport = 'metro';")
            elif daytime == 3:
                cursor.execute("UPDATE merged_ways AS mv \
                                SET reverse_costs = cc.avg_wait_time_night, costs = 0 \
                                FROM conn_costs AS cc \
                                WHERE mv.line_number = cc.line_number \
                                AND mv.connector = 1\
                                AND mv.transport = 'metro';")
