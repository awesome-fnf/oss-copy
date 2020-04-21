# generate random Gaussian values
from random import seed
from random import gauss
# seed random number generator
seed(1)
# generate some Gaussian values
for _ in range(100):
	value = gauss(64, 64)
	print(value)