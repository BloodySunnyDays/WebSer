# -*- coding:utf-8 -*-


__author__ = 'Djj'

import sys
import os
import ConfigParser
import json
import DbIntf
import datetime



import soaplib
from soaplib.core.util.wsgi_wrapper import run_twisted #发布服务
from soaplib.core.server import wsgi
from soaplib.core.service import DefinitionBase  #所有服务类必须继承该类
from soaplib.core.service import soap
from soaplib.core.model.primitive import Integer,String
from soaplib.core.model.clazz import Array #声明要使用的类型
from soaplib.core.model.clazz import ClassModel  #若服务返回类，该返回类必须是该类的子类

# 准备数据
def cur_file_dir():
     path = sys.path[0]
     if os.path.isdir(path):
         return path
     elif os.path.isfile(path):
         return os.path.dirname(path)

sDir = cur_file_dir() + '\\Config.ini'
config = ConfigParser.ConfigParser()
config.read(sDir)

# 获取路径
def cur_file_dir():
     path = sys.path[0]
     if os.path.isdir(path):
         return path
     elif os.path.isfile(path):
         return os.path.dirname(path)

sIP        = config.get("conn", "IP")
sDateBase  = config.get('conn', 'DateBase')
sUser      = config.get('conn', 'User')
sPassw     = config.get('conn', 'Paswd')
sWebSerIp  = config.get('conn', 'WebSerIp')
#配置参数 可续费时间
stime1_s = config.get('conn','time1_Start')  #0115
stime1_e = config.get('conn','time1_End')    #0601
stime2_s = config.get('conn','time2_Start')  #0715
stime2_e = config.get('conn','time2_End')    #1201
#可续费有效期
sMaxYear = config.get('conn','MaxYear')   #2

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


class WorldServiceIntf(DefinitionBase):  #this is a web service
    @soap(String,String,_returns=String)    #声明一个服务，标识方法的参数以及返回值
    def FindUser(self,name,CertNo):
        sSQL = "select AccName,CertNo,BeginDate,InvaliDate,RegDate,a.AccTypeCode as AccTypeCode,AccTypeName,CardStatus, " \
                              "(case when ((("+stime1_s+" < (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','')))  " \
                              "and (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','') < "+stime1_e+")) or  " \
                              "(("+stime2_s+" < (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','')))  " \
                              "and (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','') < "+stime2_e+")) or " \
                              "(dateadd(year,"+sMaxYear+",InvaliDate) < GETDATE())) then '0' " \
                              "else '1' end) as CanAdd " \
                              " from IC_AccInfo a,IC_AccType b where a.AccTypeCode = b.AccTypeCode and " \
                              " CertNo= ? and AccName = ? and CardStatus = 0  "


        fname = 'log'+str(datetime.date.today()) + '.txt'
        sDir = cur_file_dir() + '\\log\\'+fname

        with open(sDir, 'a') as f:
            f.write(sSQL + ':'+ str(name.encode('gbk')) +','+ CertNo +'\n')

        user =  DbIntf.select(sSQL,CertNo,name)

        if len(user) ==0:
            return 'No Date'

        # AccName = user[0]['AccName'].encode('raw_unicode_escape')
        AccName = user[0]['AccName'].encode('raw_unicode_escape')
        AccName = AccName.decode('gbk')
        RegDate = str(user[0]['RegDate'])
        BeginDate = str(user[0]['BeginDate'])
        InvaliDate = str(user[0]['InvaliDate'])
        AccTypeName = user[0]['AccTypeName'].encode('raw_unicode_escape')
        AccTypeName = AccTypeName.decode('gbk')
        CanAdd = str(user[0]['CanAdd'])

        user[0]['RegDate'] = RegDate
        user[0]['BeginDate'] = BeginDate
        user[0]['InvaliDate'] = InvaliDate
        user[0]['AccName'] = AccName
        user[0]['AccTypeName'] = AccTypeName
        user[0]['CanAdd'] = CanAdd

        data_string = json.dumps(user[0], ensure_ascii=False)
        return data_string


    @soap(String,String,String,_returns=String)    #声明一个服务，标识方法的参数以及返回值
    def FindUser_SM(self,name,CertNo,CardNo):
        sSQL = "SELECT CustName,PaperNo,CardNO,CardTime,EndDate,PaperType,Times,TradeType,OLDCARDNO,RSRV1,ModTime, " \
                              "(case when ((("+stime1_s+" < (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','')))  " \
                              "and (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','') < "+stime1_e+")) or  " \
                              "(("+stime2_s+" < (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','')))  " \
                              "and (replace(str(DATEPART(month,GetDate())) + str(DATEPART(day,GetDate())),' ','') < "+stime2_e+")) or " \
                              "(dateadd(year,"+sMaxYear+",EndDate) < GETDATE())) then '0' " \
                              "else '1' end) as CanAdd " \
                              " from SM_AccInfo a  where  " \
                              " CustName = ? and (PaperNO= ? or CardNO = ? ) and TradeType = 1  "


        fname = 'log'+str(datetime.date.today()) + '.txt'
        sDir = cur_file_dir() + '\\log\\'+fname

        with open(sDir, 'a') as f:
            f.write(sSQL + ':'+ str(name.encode('gbk')) +','+ CertNo +','+CardNo +'\n')

        user =  DbIntf.select(sSQL,name,CertNo,CardNo)

        if len(user) ==0:
            return 'No Date'

        # AccName = user[0]['AccName'].encode('raw_unicode_escape')
        AccName = user[0]['CustName'].encode('raw_unicode_escape')
        AccName = AccName.decode('gbk')
        ModTime = str(user[0]['ModTime'])
        CardTime = str(user[0]['CardTime'])
        EndDate = str(user[0]['EndDate'])

        CanAdd = str(user[0]['CanAdd'])

        user[0]['ModTime'] = ModTime
        user[0]['CardTime'] = CardTime
        user[0]['EndDate'] = EndDate
        user[0]['CustName'] = AccName
        user[0]['CanAdd'] = CanAdd

        data_string = json.dumps(user[0], ensure_ascii=False)
        return data_string

    # @soap(String,String,String,_returns=String)
    # def AddMoney(self,CertNo,tradeid,accType):
    #     AccType = DbIntf.select('select tradelimitcount from IC_AccType '
    #                           ' where acctypecode = ?',accType)
    #     if len(AccType) ==0:
    #         return '没有对应卡类型'
    #
    #     tradelimitcount = AccType[0]['tradelimitcount']
    #
    #
    #     AccInfo = DbIntf.select('select InvaliDate,CardStatus from IC_AccInfo '
    #                           ' where CertNo = ?',CertNo)
    #
    #     if len(AccInfo) ==0:
    #         return '没有对应卡'
    #
    #     if int(AccInfo[0]['CardStatus']) <> 0 :
    #         return '该卡不在正常状态！'
    #
    #     InvaliDate = str(AccInfo[0]['InvaliDate'])
    #     NewInvaliDate = datetime.datetime.strptime(InvaliDate, "%Y-%m-%d %H:%M:%S").date()
    #
    #
    #
    #     FeeType = DbIntf.select('select FeeCode,usemonth,FeeSum from IC_FeeType '
    #                           ' where acctypecode = ? and feeopttype = 05 ',accType)
    #
    #     if len(FeeType) ==0:
    #         return '没有对应费用类型'
    #
    #     FeeCode = FeeType[0]['FeeCode'].encode('raw_unicode_escape')
    #     usemonth = int(FeeType[0]['usemonth'])
    #     FeeSum = float(FeeType[0]['FeeSum'])
    #
    #     if NewInvaliDate >= datetime.date.today():
    #         NewInvaliDate = datetime_offset_by_month(NewInvaliDate,usemonth)
    #     else:
    #         NewInvaliDate = datetime_offset_by_month(datetime.date.today(),usemonth)
    #
    #     AccInfo = DbIntf.select('select id,cardno from IC_AccInfo '
    #                           ' where certno = ? ',CertNo)
    #     if len(AccInfo) ==0:
    #         return '查无此卡'
    #     AccNo = AccInfo[0]['id']
    #     cardno = AccInfo[0]['cardno'].encode('raw_unicode_escape')
    #
    #     #数据准备完毕
    #
    #     # 封装在一个事务
    #     @DbIntf.with_transaction
    #     def update_profile(AccNo, cardno,FeeCode,tradeid,tradelimitcount,CertNo,rollback):
    #         u1 = dict(AccNo=AccNo, cardno=cardno, tradedate=str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
    #                   paytype='01', opttype='0', FeeCode=FeeCode,FeeSum = FeeSum,
    #                   parkcode='0',OptorCode='999',remark=tradeid)
    #         DbIntf.insert('IC_TradeDetail', **u1)
    #         DbIntf.update('update IC_AccInfo set leftcount= leftcount + ?,InvaliDate = ? where certno=?',
    #                       tradelimitcount,NewInvaliDate.strftime('%Y-%m-%d %H:%M:%S'), CertNo)
    #         if rollback:
    #             raise StandardError('will cause rollback...')
    #
    #     try:
    #         update_profile(AccNo, cardno,FeeCode,tradeid,tradelimitcount,CertNo,False)
    #     except ImportError:
    #         return '数据提交失败'
    #
    #     return 'success'

    @soap(String,String,String,String,_returns=String)
    def AddMoney(self,CertNo,tradeid,accType,StartDate ='' ,EndDate = ''):
        try:
            fname = 'log'+str(datetime.date.today()) + '.txt'
            sDir = cur_file_dir() + '\\log\\'+fname

            with open(sDir, 'a') as f:
                f.write('DECLARE @RetMess varchar '
                           'exec WebService_AddMoney "'+CertNo+'","'+tradeid+'","'+accType+'",Null,Null,'
                           '@RetMess OUTPUT '
                           'SELECT @RetMess ' +'\n')

            RES = DbIntf.exec_sp('DECLARE @RetMess varchar '
                           'exec WebService_AddMoney "'+CertNo+'","'+tradeid+'","'+accType+'",Null,Null,'
                           '@RetMess OUTPUT '
                           'SELECT @RetMess ')

            RetMess = RES[0]['RetMess'].encode('raw_unicode_escape')
            RetMess = RetMess.decode('gbk')
            data_string = json.dumps(RetMess, ensure_ascii=False)
            return data_string

        except ImportError:
            return '数据提交失败'


    @soap(String,String,String,String,_returns=String)
    def OpenCard(self,CertNo,tradeid,FeeSum,Name):
        try:
            fname = 'log'+str(datetime.date.today()) + '.txt'
            sDir = cur_file_dir() + '\\log\\'+fname

            with open(sDir, 'a') as f:
                f.write('DECLARE @RetMess varchar '
                           'exec WebService_OpenCard "'+CertNo+'","'+tradeid+'","'+FeeSum+'","'+str(Name.encode('gbk'))+'",'
                           '@RetMess OUTPUT '
                           'SELECT @RetMess ' +'\n')

            RES = DbIntf.exec_sp('DECLARE @RetMess varchar '
                           'exec WebService_OpenCard "'+CertNo+'","'+tradeid+'","'+FeeSum+'","'+Name.encode('utf-8')+'",'
                           '@RetMess OUTPUT '
                           'SELECT @RetMess ')

            RetMess = RES[0]['RetMess'].encode('raw_unicode_escape')
            RetMess = RetMess.decode('gbk')
            data_string = json.dumps(RetMess, ensure_ascii=False)
            return data_string

        except ImportError:
            return '数据提交失败'

        # return 'success'

#网络SOAP 处理提高效率
def Run_Tws():
    soap_app=soaplib.core.Application([WorldServiceIntf], 'tns')
    wsgi_app=wsgi.Application(soap_app)
    # print 'listening on 218.4.64.93:8092'
    # print 'wsdl is at: http://218.4.64.93:8092/SOAP/?wsdl'
    run_twisted( ( (wsgi_app, "SOAP"),), 8092)


def Run_Ser():
    try:
        from wsgiref.simple_server import make_server
        soap_application = soaplib.core.Application([WorldServiceIntf], 'tns')
        wsgi_application = wsgi.Application(soap_application)
        server = make_server(sWebSerIp, 8092, wsgi_application)
        server.serve_forever()

    except ImportError:
        print 'WebService error'
        raw_input()


def GetDBConnet():
    try:
        print 'version 160602'
        print sUser
        print sDateBase
        print sIP
        DbIntf.create_engine(user=sUser, password=sPassw, database=sDateBase, host=sIP)
        return True
    except ImportError:
        print 'DateBaseServer error'
        raw_input()


if GetDBConnet():
    Run_Tws()
