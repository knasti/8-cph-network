import time
import import_shp_to_db, import_conn_to_db
from database import Database
from database import CursorFromConnectionFromPool
from train import Train
from metro import Metro
from bus import Bus
from pedestrian import Pedestrian

# Connecting to the database
Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')

start_time = time.time()

#with CursorFromConnectionFromPool() as cursor:
#    cursor.execute("SET search_path = current, public;")


# Initiating the transport classes
train = Train()
metro = Metro()
bus = Bus()
pedestrian = Pedestrian(1.39) # 1.39 m/s = 5 km/h

# Importing shapefiles into the db, only run this the first time or in case of changed shapefiles
# 0 = current network, 1 = future network
import_shp_to_db.import_shp_to_db(0)

# Importing connector costs into a table in postgres, only run the first time or if the connector costs have changed
import_conn_to_db.import_conn_to_db()

# Updates all of the calculated costs
train.update_time_calc_costs(2, 2, 20)
metro.update_time_calc_costs(2, 2, 20)
bus.update_time_calc_costs()

# Applying moving costs to the objects
train.update_moving_costs()
metro.update_moving_costs()
bus.update_moving_costs()
pedestrian.update_moving_costs()

# Applying connector costs to the objects, 0 = rh, 1 = day, 2 = evening, 3 = night
train.update_conn_costs(1)
metro.update_conn_costs(1)
bus.update_conn_costs(1)

print("--- %s seconds ---" % (time.time() - start_time))