from itertools import product
for y, x in product(range(3), repeat=2):
  print('Im y ' + str(y))
  print('Im x ' + str(x))
  #print(x)