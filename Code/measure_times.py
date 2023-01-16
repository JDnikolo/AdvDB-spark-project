import csv
from queries import *
import sys
import time
import datetime
# only execute this from the Code folder!


def execAll():
    times = {}
    times["execTime"] = datetime.datetime.now()
    times["Q1total"], times["Q1postRead"], res = executeQ1(standalone=True)
    print(f"{datetime.datetime.now()}: Q1 done.")
    times["Q2total"], times["Q2postRead"], res = executeQ2(standalone=True)
    print(f"{datetime.datetime.now()}: Q2 done.")
    times["Q3APItotal"], times["Q3APIpostRead"], res = executeQ3API(
        standalone=True)
    print(f"{datetime.datetime.now()}: Q3API done.")
    times["Q3RDDtotal"], times["Q3RDDpostRead"], res = executeQ3RDD(
        standalone=True)
    print(f"{datetime.datetime.now()}: Q3RDD done.")
    times["Q4total"], times["Q4postRead"], res = executeQ4(standalone=True)
    print(f"{datetime.datetime.now()}: Q4 done.")
    times["Q5total"], times["Q5postRead"], res = executeQ5(standalone=True)
    print(f"{datetime.datetime.now()}: Q5 done.")
    return times


def exec4perhour7days():
    with open('time/log.txt', 'a') as sys.stdout:
        print(f"Log Start: {datetime.datetime.now()}")
    start = time.time()
    columns = ['execTime',
               'Q1total', 'Q1postRead',
               'Q2total', 'Q2postRead',
               'Q3APItotal', 'Q3APIpostRead',
               'Q3RDDtotal', 'Q3RDDpostRead',
               'Q4total', 'Q4postRead',
               'Q5total', 'Q5postRead']
    with open('time/times.csv', 'a') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
    while (time.time()-start < 60*60*7):
        start = time.time()
        with open('time/log.txt', 'a') as sys.stdout:
            print(f"{datetime.datetime.now()}:All done, sleeping.")
            times = execAll()
        with open('time/times.csv', 'a') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writerow(times)
        with open('time/log.txt', 'a') as sys.stdout:
            print(f"{datetime.datetime.now()}:All done, sleeping.")
        elapsed = time.time()-start
        time.sleep(15*60-elapsed)


def exec4perhour7days1worker():
    with open('time/log1w.txt', 'a') as sys.stdout:
        print(f"Log Start: {datetime.datetime.now()}")
    start = time.time()
    columns = ['execTime',
               'Q1total', 'Q1postRead',
               'Q2total', 'Q2postRead',
               'Q3APItotal', 'Q3APIpostRead',
               'Q3RDDtotal', 'Q3RDDpostRead',
               'Q4total', 'Q4postRead',
               'Q5total', 'Q5postRead']
    with open('time/times1w.csv', 'a') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
    while (time.time()-start < 60*60*7):
        start = time.time()
        with open('time/log1w.txt', 'a') as sys.stdout:
            print(f"{datetime.datetime.now()}:All done, sleeping.")
            times = execAll()
        with open('time/times1w.csv', 'a') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writerow(times)
        with open('time/log1w.txt', 'a') as sys.stdout:
            print(f"{datetime.datetime.now()}:All done, sleeping.")
        elapsed = time.time()-start
        time.sleep(15*60-elapsed)


if __name__ == "__main__":
    if '1worker' in sys.argv:
        exec4perhour7days1worker()
    else:
        exec4perhour7days()
