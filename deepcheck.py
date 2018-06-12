#!/usr/bin/python
# -*- coding: utf-8 -*-

import time, urllib, csv, re, time, sys
from operator import  itemgetter
import MySQLdb as mdb
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
            
  cur.execute("SELECT fundname, isin_no, label, currency, funded_since from fundproduct where prod_id="+tag)
  _product = cur.fetchall()
  if (len(_product) == 0): exit(0)
  product = _product[0]
#  print product
  cur.execute("SELECT date,asset_price from assets where prod_id="+tag+" order by date desc limit "+str(limit))
  x_matrix = cur.fetchall()
  if (len(x_matrix) <615): exit(0)
  
#  print len(x_matrix), len(y_matrix), len(x_matrix[0]), len (y_matrix[0])
  
  gaps = 0
  curr_gap_price = 0
  curr_gap_days = 0
  days_since_lowest_price = 0
  curr_gap_start = ''
  lowest_price_in_gap = x_matrix[len(x_matrix)-1][1]
  last_price = x_matrix[len(x_matrix)-1][1]
  last_day = x_matrix[len(x_matrix)-1][0]
  in_gap = 0
  _GAPS=[]
  for row, i in reversed(list(enumerate(x_matrix))):
#     print row, x_matrix[row][0], x_matrix[row][1], in_gap, last_price
     if (x_matrix[row][1] < last_price):
        if (in_gap == 0):
#          print "gap_started (%d)" % gaps
          in_gap = 1
          curr_gap_price = last_price
          curr_gap_days = 1
          lowest_price_in_gap = x_matrix[row][1]
          curr_gap_start = last_day
          days_since_lowest_price = 0          
        else:
          if (x_matrix[row][1] < lowest_price_in_gap): 
             lowest_price_in_gap = x_matrix[row][1]
             days_since_lowest_price = 0
          curr_gap_days += 1
          days_since_lowest_price += 1
     if (in_gap and x_matrix[row][1] > curr_gap_price):
#        print "gap closed (%d)" % gaps 
#        print "days: ",curr_gap_days," high: ",curr_gap_price, " low: ",lowest_price_in_gap
#        _GAPS.append([([gaps],[curr_gap_days],[curr_gap_price],[lowest_price_in_gap],[curr_gap_price-lowest_price_in_gap])])
        _GAPS.append([[int(gaps)],[int(curr_gap_days)],[float(curr_gap_price)],[float(lowest_price_in_gap)],[1-float(lowest_price_in_gap)/float(curr_gap_price)],[curr_gap_start],[x_matrix[row][0]],[days_since_lowest_price]])
        gaps+=1
        in_gap = 0
        
     last_price = x_matrix[row][1]
     last_day = x_matrix[row][0]

  if (in_gap):
 #    print "stay on gap! (close) %d" % gaps
 #    print "days: ",curr_gap_days," high: ",curr_gap_price, " low: ",lowest_price_in_gap
     _GAPS.append([[int(gaps)],[int(curr_gap_days)],[float(curr_gap_price)],[float(lowest_price_in_gap)],[1-float(lowest_price_in_gap)/float(curr_gap_price)],[curr_gap_start],[last_day],[days_since_lowest_price]])
  
  biggest_gap_days= 0
  biggest_gap_days_id = -1
  biggest_gap_pricediff = 0
  biggest_gap_pricediff_id =-1
  days_in_gaps=0
  for row, i in enumerate(_GAPS):
     days_in_gaps+=(_GAPS[row][1])[0]
     if _GAPS[row][1] > biggest_gap_days: 
       biggest_gap_days = _GAPS[row][1]
       biggest_gap_days_id = row
     if _GAPS[row][4] > biggest_gap_pricediff: 
       biggest_gap_pricediff = _GAPS[row][4]
       biggest_gap_pricediff_id = row
       
#  print gaps
#  print biggest_gap_days, _GAPS[biggest_gap_days_id]
#  print biggest_gap_pricediff, _GAPS[biggest_gap_pricediff_id]
  

#  print x_matrix[5]
#  print x_matrix[0]
  print "%s%s\"%s\"%s%s%.04f%.04f%.04f%.04f%.04f%.04f%d%d%d%.04f%d%d%.04f%d" % (
       product[1],product[2], product[0], product[3], product[4], 
       (float(str(x_matrix[0][1]))/float(str(x_matrix[1][1]))-1),
       (float(str(x_matrix[0][1]))/float(str(x_matrix[4][1]))-1),
       (float(str(x_matrix[0][1]))/float(str(x_matrix[19][1]))-1),
       (float(str(x_matrix[0][1]))/float(str(x_matrix[110][1]))-1),
       (float(str(x_matrix[0][1]))/float(str(x_matrix[220][1]))-1),
       (float(str(x_matrix[0][1]))/float(str(x_matrix[600][1]))-1),
       gaps,days_in_gaps,biggest_gap_days[0],
       float((_GAPS[biggest_gap_days_id][4])[0]),
       float((_GAPS[biggest_gap_days_id][7])[0]),
       float((_GAPS[biggest_gap_pricediff_id][1])[0]),
       biggest_gap_pricediff[0],
       float((_GAPS[biggest_gap_pricediff_id][7])[0])
       )