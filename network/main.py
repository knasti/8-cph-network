from database import Database
from train import Train
from pedestrian import Pedestrian

Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')

train = Train(1.15, -1.15, 19)

pedestrian = Pedestrian(1.39) # 1.39 m/s = 5 km/h

print(train.load_length_from_db())

print(pedestrian.load_length_from_db())

print(pedestrian.calculate_moving_costs())

'''
Database.initialise(user='postgres', password='postgres', host='localhost', database='public_transport_2')

train = Train(6)
print(train.spatial_length)

print(train.calculate_moving_costs(19, 1.15, -1.15))

print(train.calculate_moving_costs(60, 8.15, -1.15))

'''