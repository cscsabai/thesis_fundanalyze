#!/usr/bin/python
# -*- coding: utf-8 -*-

import time, urllib, csv, re, time, sys
from operator import  itemgetter
import MySQLdb as mdb
import numpy as np
import datetime as dt

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
            
  cur.execute("SELECT fundname, isin_no, label, currency, funded_since from fundproduct where prod_id="+tag)
  _product = cur.fetchall()
  if (len(_product) == 0): exit(0)
  product = _product[0]
#  print product
  cur.execute("SELECT date,asset_price from assets where prod_id="+tag+" order by date desc limit "+str(limit))
  x_matrix = cur.fetchall()
  if (len(x_matrix) <615): 
     print "Not enought historical data to analyse it!"
  
  _matrix = []
#Calculate moving averages
  for row, i in reversed(list(enumerate(x_matrix))):
     _c_ma10 = 0.0
     _c_ma30 = 0.0
     _c_rsiplus = 0.0
     _c_rsiminus = 0.0
     _c_rsi = 0.0
     if (len(x_matrix)-row >= 15):
       for j in range(0,14): 
         if (float(x_matrix[row+j][1]) > float(x_matrix[row+j+1][1])):
            _c_rsiplus += float(x_matrix[row+j][1]) - float(x_matrix[row+j+1][1])
         else:
            _c_rsiminus += float(x_matrix[row+j+1][1]) - float(x_matrix[row+j][1])
       if (_c_rsiminus == 0):
         _c_rsi = 100
       else:
         _c_rsi = int(100-(100/(1+(_c_rsiplus/_c_rsiminus))))
     if (len(x_matrix)-row >= 10): 
       for j in range(0,10): _c_ma10 += float(x_matrix[row+j][1])
     if (len(x_matrix)-row >= 30): 
       for j in range(0,30): _c_ma30 += float(x_matrix[row+j][1])
     _matrix.append([x_matrix[row][0], float(x_matrix[row][1]), _c_ma10/10.0, _c_ma30/30.0, _c_rsi])
  

  _rawcrosses = []
  _rsi_lane = -1  #0: 30  1: 50   2: 70
  if (_matrix[0][4] < 50):
    _rsi_lane = 0
  else:
    _rsi_lane = 2
  if (_matrix[30][2]>_matrix[30][3]):
     _gdir = 10
  else:
     _gdir = 30
  _skip=0
  _inside=-1
  _money = 100000.0
  _units = _money / _matrix[0][1]
  _last_days = 0
  for row, i in enumerate(_matrix):
     if (_skip >0):
       _skip -= 1
     if (row>=30):
        if (_matrix[row][4] <= 30): _rsi_lane=0
        if (_matrix[row][4] >= 70): _rsi_lane=2
        _ldir = 0
        if (_matrix[row][2]>_matrix[row][3]): 
           _ldir = 10  
        else: 
           _ldir = 30
        if (_ldir != _gdir):  #cross!
           _type=0  # 1:golden, 2: death
           if (_matrix[row][1] > _matrix[row][3]):
               _type=1
           else:
               _type=2
                 
           _rawcrosses.append([_matrix[row][0],_type,_matrix[row][1],_matrix[row][2],_matrix[row][3]])
        _gdir = _ldir           
        if (len(_rawcrosses)-1 > 0):
#          print ["GOLDEN:","DEATH:"][_rawcrosses[len(_rawcrosses)-1][1] == 2],_matrix[row][0],_rsi_lane,_matrix[row][4]
          if (_matrix[row][4] >=50 and _matrix[row-1][4] < 50 and _rsi_lane == 0 and _inside!=1 and _rawcrosses[len(_rawcrosses)-1][1] == 1):
            print "BESZALLO... (T+3): ",_matrix[row+3][0],_matrix[row+3][1]
            _units = _money / _matrix[row+3][1]
            _skip = 4
            _inside = 1
            _last_days = row
          if (_matrix[row][4] <=50 and _matrix[row-1][4] > 50 and _inside == 1 and _skip ==0):
            print "KISZALLO: ",_matrix[row+3][0],_matrix[row+3][1],row-_last_days
            _money = _units * _matrix[row+3][1]
            _inside = 0
          if (_inside == 1 and _rawcrosses[len(_rawcrosses)-1][1] == 2):
            print "EMERGENCY EXIT! T+",_matrix[row+3+_skip][0],_skip+3," on: ",_matrix[row+3+_skip][1], row-_last_days
            _money = _units * _matrix[row+3+_skip][1]
            _skip = 4
            _inside = 0
#          print ["GOLDEN:","DEATH:"][_rawcrosses[len(_rawcrosses)-1][1]]

  if (_inside == 1):
    _money =_units * _matrix[len(_matrix)-1][1]  
  print "Natural increment[%s]: (%.06f -> %.06f) %.03f%%"%(_matrix[0][0],_matrix[0][1],_matrix[len(_matrix)-1][1],(_matrix[len(_matrix)-1][1]/_matrix[0][1]-1.0)*100)
  print "Analysed increment: (%.06f -> %.06f) %.03f%%"%(100000,_money,((_money/100000)-1)*100)
