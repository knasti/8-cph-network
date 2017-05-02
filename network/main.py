from database import Database
from train import Train

Database.initialise(user='postgres', password='postgres', host='localhost', database='public_transport_2')

train = Train(6)
print(train.spatial_length)

print(train.calculate_moving_costs(19, 1.15, -1.15))

print(train.calculate_moving_costs(60, 8.15, -1.15))


Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')

train2 = Train(5)

train2.calculate_conn_costs('f', 300)
