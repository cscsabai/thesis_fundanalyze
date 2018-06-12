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
     print("Usage: ./execute --xtag '' --ytag '' [--debug]")
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
            
  cur.execute("SELECT date,asset_price from assets where prod_id="+xtag+" order by date desc limit 600")
  x_matrix = cur.fetchall()

  cur.execute("SELECT date,asset_price from assets where prod_id="+ytag+" order by date desc limit 600")
  y_matrix = cur.fetchall()
  
  print len(x_matrix), len(y_matrix), len(x_matrix[0]), len (y_matrix[0])
  x_y_uniq = column(x_matrix,0) + list(set(column(y_matrix,0)) - set(column(x_matrix,0)))
  x_y_uniq.sort()
  
  x_last = x_matrix[len(x_matrix)-1][1]
  y_last = y_matrix[len(y_matrix)-1][1]
  X = []
  Y = []
  for row, i in enumerate(x_y_uniq):
     X.append(float(x_last))
     Y.append(float(y_last))     
     if (find(x_matrix, x_y_uniq[row]) != -1): x_last = x_matrix[find(x_matrix, x_y_uniq[row])][1]
     if (find(y_matrix, x_y_uniq[row]) != -1): y_last = y_matrix[find(y_matrix, x_y_uniq[row])][1]
     
  _SUMdiff = 0.0
  _Xstddev = []
  for row, i in enumerate (X):
     if row%20 == 0:
      _SUMdiff += ((X[row]/X[row-20]-1)*100) - ((Y[row]/Y[row-20]-1)*100)  
      _Xstddev.append((X[row]/X[row-20]-1)*100)
#      print row,X[row],Y[row], (X[row]/X[row-1]-1),(Y[row]/Y[row-1]-1), (X[row]/X[row-1]-1) - (Y[row]/Y[row-1]-1), _SUMdiff
  print "uppersum",_SUMdiff
  _recSUMdiff= float(float(1/float(row))*_SUMdiff)
  print "1/n(uppersum) %d %.08f %.08f %.08f"%(row,_SUMdiff,(1/float(row)), _recSUMdiff)                   
  print "stddev:",np.std(_Xstddev)
  print "Portfolio return:",(X[row]/X[0]-1)*100
  print "Risk free portfolio return:",(Y[row]/Y[0]-1)*100
  print "Extra return over risk-free profit:",((X[row]/X[0]-1)*100)-((Y[row]/Y[0]-1)*100)
  print "Sharpe (1/n) ratio:",_recSUMdiff/np.std(_Xstddev)
  print "Sharpe ratio:",_SUMdiff/np.std(_Xstddev)
