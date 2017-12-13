import matplotlib.pyplot as plt
import matplotlib.animation as animation

import pytz
from datetime import datetime, timedelta

def dt_parse(t):
    ret = datetime.strptime(t[0:19],'%Y-%m-%d %H:%M:%S')
    if t[23]=='+':
        ret-=timedelta(hours=int(t[24:26]), minutes=int(t[27:]))
    elif t[23]=='-':
        ret+=timedelta(hours=int(t[24:26]), minutes=int(t[27:]))
    return ret.replace(tzinfo=pytz.UTC)


fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)
fig.canvas.set_window_title('Monitoring streaming application')
fig.suptitle('Results preview')
ax1.set_facecolor('black')


def animate(i):
    pullData = open("/home/vieira/ProjectStreamingHIL/ResultsPreview/resultsPreview.csv","r").read()
    dataArray = pullData.split('\n')
    xar = []
    yar = []
    for eachLine in dataArray:
        if len(eachLine)>1:
            date_time,dataflow_output = eachLine.split(';')
            xar.append(dt_parse(date_time))
            yar.append(int(float(dataflow_output)))
    ax1.clear()
    ax1.plot(xar,yar,color="lime")
    ax1.set_xlabel('Date-time')
    ax1.set_ylabel('Dataflow output')	
    fig.autofmt_xdate()

ani = animation.FuncAnimation(fig, animate, interval=10000)
plt.show()

