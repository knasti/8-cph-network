from database import Database
from train import Train

Database.initialise(user='postgres', password='postgres', host='localhost', database='public_transport_2')

train = Train(6)
print(train.spatial_length)
print(train.load_length_from_db())

print(train.spatial_length[0] * 2)

print(train.calculate_costs(19, 1.15, -1.15))

print(train.calculate_costs(60, 8.15, -1.15))
