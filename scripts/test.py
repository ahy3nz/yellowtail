import numpy
import datetime

print("HELLOWORLD")
print(datetime.datetime.now())
with open("../output/test.txt", 'w') as f:
    f.write(str(datetime.datetime.now()))
