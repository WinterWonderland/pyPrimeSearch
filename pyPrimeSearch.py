import os
import sys
import math
import time
import multiprocessing
import threading
import psutil
from PySide2.QtWidgets import QApplication

from database import PrimeDatabase
from main_window import MainWindow


database_path = os.path.dirname(os.path.abspath(__file__)) + "/primes.sqlite"

class PrimeFeeder(multiprocessing.Process):
    def __init__(self, stop_flag, job_queue, start_value, block_size):
        self.stop_flag = stop_flag
        self.job_queue = job_queue
        self.start_value = start_value
        self.block_size = block_size
        super().__init__()
    
    def run(self):
        threading.main_thread().name = "Feeder"
        while not self.stop_flag.value:
            try:
                self.job_queue.put((self.start_value, self.start_value + self.block_size.value), block=True, timeout=1.0)
                self.start_value += self.block_size.value
            except:
                pass               

                
class PrimeWorker(multiprocessing.Process):
    def __init__(self, stop_flag, job_queue, prime_queue):
        self.stop_flag = stop_flag
        self.job_queue = job_queue
        self.prime_queue = prime_queue
        self.block_times = multiprocessing.Queue(maxsize=10)
        super().__init__()
        
    def run(self):
        threading.main_thread().name = "Worker"
        while not self.stop_flag.value:
            try:
                start_time = time.perf_counter()
                
                start_value, end_value = self.job_queue.get(timeout=1.0)
                for test_value in range(start_value, end_value):
                    if self.check_prime(test_value):
                        self.prime_queue.put(test_value)
                self.job_queue.task_done()
                
                end_time = time.perf_counter()
                if self.block_times.full():
                    self.block_times.get()
                self.block_times.put(end_time - start_time)
            except:
                pass
                
    @classmethod
    def check_prime(self, test_value):
        if test_value % 2 == 0 and test_value > 2:
            return False
        return all(test_value % i for i in range(3, int(math.sqrt(test_value)) + 1, 2))

    def get_block_times(self):
        block_times = []
        while not self.block_times.empty():
            block_times.append(self.block_times.get())
        return block_times
        
  
class PrimeSaver(multiprocessing.Process):
    def __init__(self, stop_flag, prime_queue, primes_database):
        self.stop_flag = stop_flag
        self.prime_queue = prime_queue
        self.primes_database = primes_database
        super().__init__()
        
    def run(self):
        threading.main_thread().name = "Saver"
        while not self.stop_flag.value:
            values = []
            while not self.prime_queue.empty():
                values.append(self.prime_queue.get())
            self.primes_database.save_prime_list(values)
            for value in values:
                self.prime_queue.task_done()
            time.sleep(0.1)
            
            
class PrimeController(threading.Thread):
    def __init__(self, main_window):
        self.stop = False
        self.main_window = main_window
        super().__init__(name="Controller")
            
    def run(self):
        primes_database = PrimeDatabase(database_path)
        start_value = primes_database.get_max_prime() + 1
     
        worker_count = multiprocessing.cpu_count()
        job_queue = multiprocessing.JoinableQueue(maxsize=worker_count * 2)
        prime_queue = multiprocessing.JoinableQueue()
     
     
        # start feeder process
        stop_flag_feeder = multiprocessing.Value("b", False)
        block_size_feeder = multiprocessing.Value("i", 1)
        feeder = PrimeFeeder(stop_flag_feeder, 
                             job_queue,
                             start_value,
                             block_size_feeder)
        feeder.daemon = True
        feeder.start()
        psutil.Process(pid=feeder.pid).nice(psutil.HIGH_PRIORITY_CLASS)
     
     
        # start worker process
        stop_flag_worker = multiprocessing.Value("b", False)
        workers = []
        for _ in range(worker_count):
            worker = PrimeWorker(stop_flag_worker, 
                                 job_queue,
                                 prime_queue)
            worker.daemon = True
            worker.start()
            workers.append(worker)
     
     
        # start saver process
        stop_flag_saver = multiprocessing.Value("b", False)
        saver = PrimeSaver(stop_flag_saver, 
                           prime_queue,
                           primes_database)
        saver.daemon = True
        saver.start()
        psutil.Process(pid=saver.pid).nice(psutil.HIGH_PRIORITY_CLASS)
     
        # update the ui and control the feeder block size
        while not self.stop:
            start_time = time.perf_counter()
            print("")
            block_size = feeder.block_size.value
            main_window.set_block_size(block_size)
             
            block_times = []
            for worker in workers:
                block_times.extend(worker.get_block_times())
                 
            if block_times:
                prefered_block_time = 1.0
                damping = 0.7
                mean_time = sum(block_times) / len(block_times)
                main_window.set_block_time(mean_time, prefered_block_time)
                if mean_time > 0:
                    feeder.block_size.value = max(1, int(block_size * (1 + ((prefered_block_time / mean_time - 1) * damping))))
             
            main_window.set_prime_count(primes_database.get_prime_count())   
            main_window.set_max_prime(primes_database.get_max_prime())
            
            end_time = time.perf_counter()
            
            # wait sleep_factor times longer than the update of the ui
            # duration of sleep is only sleep_interval to stop faster
            sleep_interval = 0.1
            sleep_factor = 5
            update_interval = (end_time - start_time) * sleep_factor
            main_window.set_ui_update_interval(update_interval, sleep_factor)
            for _ in range(int(update_interval / sleep_interval)):
                if self.stop:
                    break
                time.sleep(sleep_interval)
                 
        stop_flag_feeder.value = True
        feeder.join()
         
        job_queue.join()
        stop_flag_worker.value = True
        for worker in workers:
            worker.join()
             
        prime_queue.join()
        stop_flag_saver.value = True
        saver.join()


def check_prime(value):
    primes_database = PrimeDatabase(database_path)
    if value > primes_database.get_max_prime():
        is_prime = PrimeWorker.check_prime(value)
    else:
        is_prime = primes_database.check_prime(value)
    return is_prime


if __name__ == "__main__":  
    threading.main_thread().name = "UserInterface"
    if (QApplication.instance() is None):
            QApplication(sys.argv)
     
    main_window = MainWindow()
    main_window.show()
     
    controller = PrimeController(main_window)
    controller.start()
     
    QApplication.instance().exec_()
    controller.stop = True
    controller.join()
    