import matplotlib.pyplot as plt
import numpy as np
import time


def plotLatency(operationCount, latencyList, stringOperation):
    """In database performance latency is the time interval between the client stimulation 
       (e.g. a read operation) and the response (e.g. return the selected items).
       Plot time intervals vs count of operations"""
    arrayLatency = np.array(latencyList)
    arrayLatency *= 1000.0  # transform to ms
    arrayOperCount = np.arange(operationCount)
    y_mean = [np.mean(arrayLatency) for i in arrayOperCount]

    fig = plt.figure()
    plt.title("Operation latency for " + stringOperation)
    plt.xlabel("Operation count")
    plt.xlim([0, len(arrayOperCount)])
    plt.ylabel("Latency (msec)")
    plt.plot(arrayOperCount, arrayLatency, '-o')
    plt.plot(arrayOperCount, y_mean, label='Mean', linestyle='solid', color='red')
    plt.yscale('log')
    plt.show(block=True)  # if you want to continue computation without blocking windows: block=False
    plt.close(fig)


def calcThroughput(operationCount, timediff):
    """Throughput is measured in database operations (read,update,delete etc...) per second.
       In order to be more accurate do not invoke the measurements.plotLatency function
       when you calculate throughput, because the plotting slows down the calculations. """

    print("Throughput: ", int(operationCount / timediff), "requests/second")

