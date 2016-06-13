# -*- coding:utf-8 -*-
__author__ = 'Djj'

from suds.client import Client

def webser():
    url = 'http://192.168.0.28:8092/SOAP/?wsdl'
    client = Client(url)
    result = client.service
    return result

websev = webser()

ws = websev.AddMoney("330781198612296332","201605040001","01")

print type(ws)