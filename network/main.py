import time
import import_shp_to_db, import_conn_to_db
from database import Database
from train import Train
from metro import Metro
from bus import Bus
from pedestrian import Pedestrian

Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')

start_time = time.time()

train = Train()

metro = Metro()

bus = Bus()

import_shp_to_db.import_shp_to_db(1)

import_conn_to_db.import_conn_to_db()


bus.calculate_moving_costs_2()


pedestrian = Pedestrian(1.39) # 1.39 m/s = 5 km/h



train.update_time_calc_costs()

metro.update_time_calc_costs()

bus.update_time_calc_costs()

print("--- %s seconds ---" % (time.time() - start_time))

train.update_moving_costs()

print("--- %s seconds ---" % (time.time() - start_time))

train.update_conn_costs(1)

print("--- %s seconds ---, LOOK FOR THIS" % (time.time() - start_time))

metro.update_moving_costs()

print("--- %s seconds ---" % (time.time() - start_time))

metro.update_conn_costs(1)

print("--- %s seconds ---, METRO" % (time.time() - start_time))

bus.update_moving_costs()

print("--- %s seconds ---" % (time.time() - start_time))

bus.update_conn_costs(1)

print("--- %s seconds ---, BUSES" % (time.time() - start_time))

pedestrian.update_moving_costs()

print("--- %s seconds ---, PEDESTRIAN" % (time.time() - start_time))

'''
Database.initialise(user='postgres', password='postgres', host='localhost', database='public_transport_2')

train = Train(6)
print(train.spatial_length)

print(train.calculate_moving_costs(19, 1.15, -1.15))

print(train.calculate_moving_costs(60, 8.15, -1.15))

'''