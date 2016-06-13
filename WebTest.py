# -*- coding:utf-8 -*-
__author__ = 'Djj'


import datetime
import os,sys


def datetime_offset_by_month(datetime1, n = 1):
    one_day = datetime.timedelta(days = 1)
    q,r = divmod(datetime1.month + n, 12)
    datetime2 = datetime.datetime(
        datetime1.year + q, r + 1, 1) - one_day
    if datetime1.month != (datetime1 + one_day).month:
        return datetime2
    if datetime1.day >= datetime2.day:
        return datetime2
    return datetime2.replace(day = datetime1.day)

from suds.client import Client
#
#

# print time.time()
# strd = test.service.FindUser(u'黄鑫军','330719198309053911')
# print time.time()
#
# print strd

# dtstr = '2014-02-14 21:32:12'
#
# print datetime.datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S").date()
# print datetime.date.today()
#
# print datetime.datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S").date() < datetime.date.today()
# print datetime.datetime.strptime(str(datetime.datetime.today()), "%Y-%m-%d %H:%M:%S")

# print datetime_offset_by_month(datetime.date.today(),12)
# print datetime.datetime.strptime(str(datetime.date.today()), "%Y-%m-%d %H:%M:%S") < datetime.datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S")


WebIP = '192.168.0.44'
# WebIP = '218.4.64.93'
# print  test.service.AddMoney('330719198309053911','9911','01')
# print "11 " + repr(WebIP) +" 22"
#
print datetime.datetime.now()
test=Client('http://'+WebIP+':8092/SOAP/?wsdl')
print datetime.datetime.now()
# print test.service.FindUser(u'黄鑫军','330719198309053911')
# print test.service.FindUser(u'严怡','320524196212084008')

print test.service.FindUser_SM(u'姓名','320908199910100001','2150999999990025')

print datetime.datetime.now()