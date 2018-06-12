#!/usr/bin/python
# -*- coding: utf-8 -*-

import time, urllib, csv, re, time, sys, math
from operator import  itemgetter
import MySQLdb as mdb
import math as math
import numpy as np

lib_path = os.path.abspath('/secure/conf')
import ccoin_conf as cconf

try:
    cconf.__sec_initialize(token="diplo-demo")   
    con = mdb.connect(cconf.host, cconf.user, cconf.pass, cconf.db)    
    
    con.autocommit(True)
            
    cur = con.cursor()
    cur.execute("SELECT VERSION()")

    ver = cur.fetchone()

except mdb.Error, e:

    print "Error %d: %s" % (e.args[0], e.args[1])
    emergency_email_warning("MYSQL connection error! Error: %d, %s" % (e.args[0], e.args[1]))
    sys.exit(1)

debug_mode = 0

def column(matrix, i):
    return [row[i] for row in matrix]

def find(l, elem):
    for row, i in enumerate(l):
       try:
           if (i[0] == elem):
              return row
       except ValueError:
           continue
    return -1

if __name__ == '__main__':

  comp_id = ""
  if len(sys.argv) <2: 
     print("Usage: ./execute --tag '' --limit <num>[--debug]")
     sys.exit(0)
  tag = ""
  limit = 0
  for ai in range(0,len(sys.argv)):
    if sys.argv[ai] == "--debug":
      debug_mode=1
    if sys.argv[ai] == "--tag":
      tag = sys.argv[ai+1]
    if sys.argv[ai] == "--limit":
      limit = int(sys.argv[ai+1])
      
  if tag == "" and limit == 0:
     print("Missing tagparameter")
     sys.exit(0)
            
  cur.execute("SELECT date,open,high,low,close from stocks where ticker='"+tag+"' order by date desc limit "+str(limit))
  _matrix = cur.fetchall()
  
  
  _last_20_days=[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
  _last_20_deviation=[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
  print "%s%s"%("date",tag)
  for row, i in reversed(list(enumerate(_matrix))):
     print "%s%.04f"%(_matrix[row][0], _matrix[row][4])
