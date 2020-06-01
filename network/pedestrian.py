from database import CursorFromConnectionFromPool


class Pedestrian:
    def __init__(self, velocity):
        self.velocity = velocity
        self.spatial_length = []
        self.id = []

    def __repr__(self):
        return "<Pedestrian>"

    def load_length_from_db(self):
        # Connecting to database
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SELECT spatial_length, pk FROM merged_ways WHERE transport = 'pedestrian'")
            pedestrian_data = cursor.fetchall() # Stores the result of the query in the pedestrian_data variable
            self.spatial_length = [] # Makes sure the list is empty
            if pedestrian_data:  # If no data it returns none which is equivalent to false in boolean expressions
                for i in range(len(pedestrian_data)): # Iterating through all of the pedestrian data
                    self.spatial_length.append(pedestrian_data[i][0]) # Removing the tuples that comes along with the DB queries
                    self.id.append(pedestrian_data[i][1])
        return None

    def calculate_moving_costs(self):
        # Loading in the spatial lengths and their ids
        self.load_length_from_db()

        # Defining times as a list, will store all times in pedestrian network
        times = []

        # Finding the time it takes to walk the different lengths
        for item in self.spatial_length:
            time = item / self.velocity
            times.append(time)

        return times

    def update_moving_costs(self):
        # Calculating the moving costs
        costs = self.calculate_moving_costs()

        for i in range(len(self.spatial_length)):
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("UPDATE merged_ways SET costs = {0}, reverse_costs = {0} \
                                WHERE pk = {1};".format(costs[i], self.id[i]))
