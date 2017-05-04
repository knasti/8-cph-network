from database import CursorFromConnectionFromPool
import formulas


class Train:
    def __init__(self, acceleration, deceleration, velocity):
        self.acceleration = acceleration
        self.deceleration = deceleration
        self.velocity = velocity
        self.spatial_length = []

    def __repr__(self):
        return "<Train {}>".format(self.value)

    def update_moving_costs(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE merged_ways SET costs = %s \
                            WHERE transport = 'train';", [self.value])

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length FROM merged_ways WHERE connector = 0 AND transport = 'train'")
            train_data = cursor.fetchall() # Stores the result of the query in the train_data variable
            self.spatial_length = [] # Makes sure the list is empty
            if train_data:  # None is equivalent to false in boolean expressions
                for i in range(len(train_data)): # Iterating through all of the train data
                    self.spatial_length.append(train_data[i][0]) # Removing the tuples that comes along with the DB queries
        return self.spatial_length

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
                # Finding the time that the trains is driving at its average speed
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

    @staticmethod
    def __load_conn_ways(line_number):
        # List to store conn_ways ids in
        conn_ways = []

        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT pk FROM merged_ways WHERE connector = 1 AND transport = 'train' AND line_number = %s", [line_number])
            conn_data = cursor.fetchall()  # Stores the result of the query in the train_data variable
            if conn_data:
                for i in range(len(conn_data)): # Iterating through all of the conn data
                    conn_ways.append(conn_data[i][0]) # Removing the tuples that comes along with the DB queries
            return conn_ways

    def calculate_conn_costs(self, line_number, avg_waiting_time):
        # Getting the ids for the train ways
        conn_ways = self.__load_conn_ways(line_number)

        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            for i in range(len(conn_ways)):
                cursor.execute('UPDATE merged_ways SET costs = %s WHERE pk = %s', (avg_waiting_time, conn_ways[i]))

