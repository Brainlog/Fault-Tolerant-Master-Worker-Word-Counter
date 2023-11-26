import glob
import logging
import os
import signal
import sys
from threading import current_thread
import time
from constants import LOGFILE, N_NORMAL_WORKERS, N_CRASHING_WORKERS, \
  N_SLEEPING_WORKERS, N_CPULIMIT, N_FILES
from mrds import MyRedis
from worker import WcWorker

workers = []

def sig_handler(signum, frame):
  for w in workers:
    w.kill()
  logging.info('Bye!')
  sys.exit()


if __name__ == "__main__":
  # Clear the log file
  open(LOGFILE, 'w').close()
  logging.basicConfig(# filename=LOGFILE,
                      level=logging.DEBUG,
                      force=True,
                      format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  thread = current_thread()
  thread.name = "client"
  logging.debug('Done setting up loggers.')

  rds = MyRedis()
  
  # signal.signal(signal.SIGTERM, sig_handler)
  signal.signal(signal.SIGINT, sig_handler)

  # Crash half the workers after they read a row
  for i in range(N_NORMAL_WORKERS):
    workers.append(WcWorker())

  for i in range(N_CRASHING_WORKERS):
    workers.append(WcWorker(crash=True))

  for i in range(N_SLEEPING_WORKERS):
    workers.append(WcWorker(slow=True, limit=N_CPULIMIT))

  t0 = time.time()
  for w in workers:
    w.create_and_run(rds=rds)
  logging.debug('Created all the workers')

  for iter, file in enumerate(glob.glob("../data34/*.csv")):
    rds.add_file(file)
    if (iter+1) % N_FILES == 0:
      logging.debug(f'Injected {iter+1} files in total so far')
      time.sleep(1)

  # Wait for workers to finish processing all the files
  while rds.is_pending():
    time.sleep(2)
    # print("hello")

  # Kill all the workers
  for w in workers:
    w.kill()

  # Wait for workers to exit
  while True:
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      break
  


  for word, c in rds.top(3):
    logging.info(f"{word.decode()}: {c}")
  logging.info(f"Total time: {time.time() - t0}")
  
  dict1 = rds.rds.hgetall("latency")
  dict2 = rds.rds.hgetall("myhash")
  list1 = []
  for key,val in dict2.items():
    list1.append([float(val.decode()),float(dict1[key].decode())])
  list1.sort()
  fil = open("data_70.txt","w")
  for i in list1:
    fil.write(str(i[1]) + "\n")
  
