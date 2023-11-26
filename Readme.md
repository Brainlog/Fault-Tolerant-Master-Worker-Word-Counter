# Cloud Computing : Streaming Computation
1. In this assignment, workers are fault tolerant. There is one more version where the master is "also" fault tolerant. Check 
2. Used Lua script to make operations atomic.
3. Used Redis as Master
To run the source code :
1. Change the path in constants.py to directory in which all the files are stored.
2. Set the number of workers.
3. To try crashing or slowing down the workers, set N_CRASHING and N_SLEEPING workers.
4. Acknowledgement : Starter code (structure of APIs) is provided by our Course Coordinator.
5. Install redis from pip.
6. Configure the redis-server on the port and run redis-client to start the master.
7. Finally run
```
python3 client.py
```
