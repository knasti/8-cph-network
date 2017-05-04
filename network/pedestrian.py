from database import CursorFromConnectionFromPool


class Pedestrian:
    def __init__(self, velocity):
        self.velocity = velocity
        self.spatial_length = []

    def __repr__(self):
        return "<Pedestrian {}>".format(self.value)

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length FROM merged_ways WHERE transport = 'pedestrian'")
            pedestrian_data = cursor.fetchall() # Stores the result of the query in the pedestrian_data variable
            self.spatial_length = [] # Makes sure the list is empty
            if pedestrian_data:  # If no data it returns none which is equivalent to false in boolean expressions
                for i in range(len(pedestrian_data)): # Iterating through all of the pedestrian data
                    self.spatial_length.append(pedestrian_data[i][0]) # Removing the tuples that comes along with the DB queries
        return self.spatial_length

    def calculate_moving_costs(self):
        # Loading in the spatial lengths
        self.load_length_from_db()

        # Defining sum_time as a list
        sum_time = []

        for i in range(len(self.spatial_length)):
            # Finding the time which it takes to achieve average speed
            time = self.spatial_length[i] / self.velocity
            sum_time.append(time)

        return sum_time

    def update_moving_costs(self):
        # Calculating the moving costs
        