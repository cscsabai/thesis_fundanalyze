#!/usr/bin/python
# -*- coding: utf-8 -*-

import time, urllib, csv, re, time, sys
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
     print("Usage: ./execute --xtag '' -yytag '' [--debug]")
     sys.exit(0)
  xtag = ""
  ytag = ""
  for ai in range(0,len(sys.argv)):
    if sys.argv[ai] == "--debug":
      debug_mode=1
    if sys.argv[ai] == "--xtag":
      xtag = sys.argv[ai+1]
    if sys.argv[ai] == "--ytag":
      ytag = sys.argv[ai+1]
      
  if ytag == "" or xtag == "":
     print("Missing xtag,ytag parameter")
     sys.exit(0)
            
  cur.execute("SELECT date,asset_price from assets where prod_id="+xtag+" order by date desc limit 200")
  x_matrix = cur.fetchall()

  cur.execute("SELECT date,asset_price from assets where prod_id="+ytag+" order by date desc limit 200")
  y_matrix = cur.fetchall()
  
  print len(x_matrix), len(y_matrix), len(x_matrix[0]), len (y_matrix[0])
  x_y_uniq = column(x_matrix,0) + list(set(column(y_matrix,0)) - set(column(x_matrix,0)))
  x_y_uniq.sort()
  
  x_last = x_matrix[len(x_matrix)-1][1]
  y_last = y_matrix[len(y_matrix)-1][1]
  X = []
  Y = []
  for row, i in enumerate(x_y_uniq):
     X.append(x_last)
     Y.append(y_last)     
     if (find(x_matrix, x_y_uniq[row]) != -1): x_last = x_matrix[find(x_matrix, x_y_uniq[row])][1]
     if (find(y_matrix, x_y_uniq[row]) != -1): y_last = y_matrix[find(y_matrix, x_y_uniq[row])][1]
     
  print np.corrcoef(X,Y)

