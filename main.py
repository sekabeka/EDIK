from auchan.result import run as auchan
from child.result import run as child
from red.result import run as red

from multiprocessing import Process

import schedule
import time

def job():
    p1 = Process(target=auchan)
    p2 = Process(target=child)
    p3 = Process(target=red)
    
    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()

schedule.every().days.at("04:00", "Europe/Moscow").do(job)
if __name__=='__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
    




        
