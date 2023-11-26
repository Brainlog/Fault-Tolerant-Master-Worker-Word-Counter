from __future__ import annotations

import logging
from typing import Optional, Final

from redis.client import Redis

from base import Worker
from constants import IN, COUNT, FNAME, LATENCY
# import time

ADDED = f'added'
MYHASH = f'myhash'
MYHASH2 = f'latency'
class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password='',
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)
    self.rds.is_pending1 = True

  def get_timestamp(self) -> float:
    timestamp = self.rds.time()
    return float(f'{timestamp[0]}.{timestamp[1]}')

  def add_file(self, fname: str) -> None:
      KEYS = [IN,FNAME,fname,MYHASH,str(self.get_timestamp())]
      member = []
      self.rds.fcall('addfile',5,*KEYS,*member)
      # print(i, " Status")

    

  def top(self, n: int) -> list[tuple[bytes, float]]:
    p = self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)
    return p

  def get_latency(self) -> list[float]:
    lat = []
    lat_data = self.rds.hgetall("latency")
    for k in sorted(lat_data.keys()):
      v = lat_data[k]
      lat.append(float(v.decode()))
    return lat

  def read(self, worker: Worker) -> Optional[tuple[bytes, dict[bytes, bytes]]]:
      entry = self.rds.xreadgroup(Worker.GROUP, worker.pid, {IN: '>'}, count=1)
      chk = 0
      if(len(entry) == 0):
        # print("I am here")
        chk = 1
        pendinglist = self.rds.xpending(IN, Worker.GROUP)
        if(pendinglist['pending']==0):
          return (-1,-1)
        start = pendinglist['min']
        entry = self.rds.xautoclaim(IN,Worker.GROUP,worker.pid,4000,start_id=start,count=1)
        if(len(entry[1])==0):
          return (-1,-1)
      filepath = ""
      mid = 0
      if chk == 0:
        filepath = entry[0][1][0][1]
        m_id = entry[0][1][0][0]
        keys= [MYHASH,filepath[FNAME].decode(),self.get_timestamp()]
        memu = []
        self.rds.fcall('reading',3,*keys, *memu)
      else:
        filepath = entry[1][0][1]
        m_id = entry[1][0][0]
       
      return m_id, filepath

  def write(self, id: bytes, wc: dict[str, int],filename) -> None:
      member_score_pairs = []
      for member, score_increment in wc.items():
          member_score_pairs.extend([score_increment, member])
      keys = [COUNT,IN,'worker',id,ADDED,FNAME,filename,LATENCY,str(self.get_timestamp()),MYHASH]
      status = self.rds.fcall('add_wc',10,*keys,*member_score_pairs)
      return None
    
  def is_pending(self) -> bool:
      return self.rds.xlen(IN) != self.rds.xlen(ADDED)
    
  def check(self,filename):
      keys = [MYHASH,filename]
      msg = []
      status = self.rds.fcall('check',2,*keys,*msg)
      status = int(status.decode())
      if(status==1):
        print(1," status")
      return status
      
  