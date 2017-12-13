import time
import datetime
import random

header = "date_time,temperature,pressure"

while True:
    date_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')
    temperature = random.uniform(1.0, 100.0) #Celsius
    pressure = random.uniform(15.0, 50.0)     #psi
    measures = header + '\n' + str(date_time) + ',' + str(temperature) + ',' + str(pressure)
    with open('/home/vieira/ProjectStreamingHIL/StreamingInput/'+str(date_time)+'.csv', 'w') as text_file:
        text_file.write(measures)
    

    time.sleep(15)
    date_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')
    temperature = random.uniform(1.0, 100.0)
    measures = header + '\n' + str(date_time) + ',' + str(temperature) + ',' + str(pressure)
    with open('/home/vieira/ProjectStreamingHIL/StreamingInput/' + str(date_time) + '.csv', 'w') as text_file:
        text_file.write(measures)
    time.sleep(15)
