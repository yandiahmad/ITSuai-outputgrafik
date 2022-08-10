from sqlalchemy import create_engine
import pymysql
import pandas as pd
import pingparsing
import datetime
from datetime import datetime, timedelta

def getConfigServer():

    sqlEngine = create_engine('mysql+pymysql://root:boss@localhost/serverdisplayoutput', pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    query = ("SELECT * FROM setupserver")

    df = pd.read_sql(query,con=dbConnection)
    
    return(df)

def getListClient(username,password,server,database,port):

    username = username
    password = password
    server = server
    database = database
    port = port 

    sqlEngine = create_engine('mysql+pymysql://' + username + ':' + password + '@' + server + '/' + database , pool_recycle=port)
    dbConnection = sqlEngine.connect()
    query = ("SELECT * FROM mastercv")

    df = pd.read_sql(query,con=dbConnection)
    print("\t Total registered client : " + str(len(df)))
    
    return(df)

def getDataClient(ipclient,database):

    # Default server andon display prod 
    username = "root"
    password = "boss"
    port = 3600

    server = ipclient
    database = database
    
    df = pd.DataFrame()
    sqlEngine = create_engine('mysql+pymysql://' + username + ':' + password + '@' + server + '/' + database , pool_recycle=port)
    dbConnection = sqlEngine.connect()
    
    query = ("select DISTINCT(outputqa.tanggalscan), outputqa.assyplan," 
             "(CASE when planJ1S1.J1S1 is not null then planJ1S1.J1S1 else 0 end) as planJ1S1, "
             "(CASE when outputJ1S1.J1S1 is not null then outputJ1S1.J1S1 else 0 end) as outputJ1S1, "
             "(CASE when planJ2S1.J2S1 is not null then planJ2S1.J2S1 else 0 end) as planJ2S1, "
             "(CASE when outputJ2S1.J2S1 is not null then outputJ2S1.J2S1 else 0 end) as outputJ2S1, "
             "(CASE when planJ3S1.J3S1 is not null then planJ3S1.J3S1 else 0 end) as planJ3S1, "
             "(CASE when outputJ3S1.J3S1 is not null then outputJ3S1.J3S1 else 0 end) as outputJ3S1, "
             "(CASE when planJ4S1.J4S1 is not null then planJ4S1.J4S1 else 0 end) as planJ4S1, "
             "(CASE when outputJ4S1.J4S1 is not null then outputJ4S1.J4S1 else 0 end) as outputJ4S1, "
             "(CASE when planJ5S1.J5S1 is not null then planJ5S1.J5S1 else 0 end) as planJ5S1, "
             "(CASE when outputJ5S1.J5S1 is not null then outputJ5S1.J5S1 else 0 end) as outputJ5S1, "
             "(CASE when planJ6S1.J6S1 is not null then planJ6S1.J6S1 else 0 end) as planJ6S1, "
             "(CASE when outputJ6S1.J6S1 is not null then outputJ6S1.J6S1 else 0 end) as outputJ6S1, "
             "(CASE when planJ7S1.J7S1 is not null then planJ7S1.J7S1 else 0 end) as planJ7S1, "
             "(CASE when outputJ7S1.J7S1 is not null then outputJ7S1.J7S1 else 0 end) as outputJ7S1, "
             "(CASE when planJ8S1.J8S1 is not null then planJ8S1.J8S1 else 0 end) as planJ8S1, "
             "(CASE when outputJ8S1.J8S1 is not null then outputJ8S1.J8S1 else 0 end) as outputJ8S1, "
             "(CASE when planOT1S1.OT1S1 is not null then planOT1S1.OT1S1 else 0 end) as planOT1S1, "
             "(CASE when outputOT1S1.OT1S1 is not null then outputOT1S1.OT1S1 else 0 end) as outputOT1S1, "
             "(CASE when planOT2S1.OT2S1 is not null then planOT2S1.OT2S1 else 0 end) as planOT2S1, "
             "(CASE when outputOT2S1.OT2S1 is not null then outputOT2S1.OT2S1 else 0 end) as outputOT2S1, "
             "(CASE when planJ1S2.J1S2 is not null then planJ1S2.J1S2 else 0 end) as planJ1S2, "
             "(CASE when outputJ1S2.J1S2 is not null then outputJ1S2.J1S2 else 0 end) as outputJ1S2, "
             "(CASE when planJ2S2.J2S2 is not null then planJ2S2.J2S2 else 0 end) as planJ2S2, "
             "(CASE when outputJ2S2.J2S2 is not null then outputJ2S2.J2S2 else 0 end) as outputJ2S2, "
             "(CASE when planJ3S2.J3S2 is not null then planJ3S2.J3S2 else 0 end) as planJ3S2, "
             "(CASE when outputJ3S2.J3S2 is not null then outputJ3S2.J3S2 else 0 end) as outputJ3S2, "
             "(CASE when planJ4S2.J4S2 is not null then planJ4S2.J4S2 else 0 end) as planJ4S2, "
             "(CASE when outputJ4S2.J4S2 is not null then outputJ4S2.J4S2 else 0 end) as outputJ4S2, "
             "(CASE when planJ5S2.J5S2 is not null then planJ5S2.J5S2 else 0 end) as planJ5S2, "
             "(CASE when outputJ5S2.J5S2 is not null then outputJ5S2.J5S2 else 0 end) as outputJ5S2, "
             "(CASE when planJ6S2.J6S2 is not null then planJ6S2.J6S2 else 0 end) as planJ6S2, "
             "(CASE when outputJ6S2.J6S2 is not null then outputJ6S2.J6S2 else 0 end) as outputJ6S2, "
             "(CASE when planJ7S2.J7S2 is not null then planJ7S2.J7S2 else 0 end) as planJ7S2, "
             "(CASE when outputJ7S2.J7S2 is not null then outputJ7S2.J7S2 else 0 end) as outputJ7S2, "
             "(CASE when planJ8S2.J8S2 is not null then planJ8S2.J8S2 else 0 end) as planJ8S2, "
             "(CASE when outputJ8S2.J8S2 is not null then outputJ8S2.J8S2 else 0 end) as outputJ8S2, "
             "(CASE when planOT1S2.OT1S2 is not null then planOT1S2.OT1S2 else 0 end) as planOT1S2, "
             "(CASE when outputOT1S2.OT1S2 is not null then outputOT1S2.OT1S2 else 0 end) as outputOT1S2 "
             "from outputqa left join (SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) as J1S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J1S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ1S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ1S1.TANGGALSCAN,outputJ1S1.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) as J2S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J2S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ2S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ2S1.TANGGALSCAN,outputJ2S1.ASSYPLAN) "
             "left join (SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J3S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J3S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ3S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ3S1.TANGGALSCAN,outputJ3S1.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J4S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J4S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ4S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ4S1.TANGGALSCAN,outputJ4S1.ASSYPLAN) "
             "left join (SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J5S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J5S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ5S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ5S1.TANGGALSCAN,outputJ5S1.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J6S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J6S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ6S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ6S1.TANGGALSCAN,outputJ6S1.ASSYPLAN) "
             "left join (SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J7S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J7S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ7S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ7S1.TANGGALSCAN,outputJ7S1.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J8S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J8S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ8S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ8S1.TANGGALSCAN,outputJ8S1.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS OT1S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'OT1S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputOT1S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputOT1S1.TANGGALSCAN,outputOT1S1.ASSYPLAN) "
             "left join (SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS OT2S1 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'OT2S1' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputOT2S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputOT2S1.TANGGALSCAN,outputOT2S1.ASSYPLAN) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,sum(J1S1) AS J1S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ1S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ1S1.PLANDATE,planJ1S1.ASSYNO) "
             "LEFT JOIN (SELECT PLANDATE, ASSYNO,SUM(J2S1) AS J2S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ2S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ2S1.PLANDATE,planJ2S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J3S1) AS J3S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ3S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ3S1.PLANDATE,planJ3S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J4S1) AS J4S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ4S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ4S1.PLANDATE,planJ4S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J5S1) AS J5S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ5S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ5S1.PLANDATE,planJ5S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J6S1) AS J6S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ6S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ6S1.PLANDATE,planJ6S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J7S1) AS J7S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ7S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ7S1.PLANDATE,planJ7S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J8S1) AS J8S1 FROM plnprod group by PLANDATE, ASSYNO) AS planJ8S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ8S1.PLANDATE,planJ8S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(OT1S1) AS OT1S1 FROM plnprod group by PLANDATE, ASSYNO) AS planOT1S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planOT1S1.PLANDATE,planOT1S1.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(OT2S1) AS OT2S1 FROM plnprod group by PLANDATE, ASSYNO) AS planOT2S1 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planOT2S1.PLANDATE,planOT2S1.ASSYNO) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) as J1S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J1S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ1S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ1S2.TANGGALSCAN,outputJ1S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) as J2S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J2S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ2S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ2S2.TANGGALSCAN,outputJ2S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J3S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J3S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ3S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ3S2.TANGGALSCAN,outputJ3S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J4S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J4S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ4S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ4S2.TANGGALSCAN,outputJ4S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J5S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J5S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ5S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ5S2.TANGGALSCAN,outputJ5S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J6S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J6S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ6S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ6S2.TANGGALSCAN,outputJ6S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J7S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J7S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ7S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ7S2.TANGGALSCAN,outputJ7S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS J8S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'J8S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputJ8S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputJ8S2.TANGGALSCAN,outputJ8S2.ASSYPLAN) "
             "left join(SELECT tanggalscan, ASSYPLAN, PERIODEOUTPUT, count(STATUS) AS OT1S2 FROM outputqa "
             "where STATUS = 'OK' and PERIODEOUTPUT = 'OT1S2' group by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN "
             "order by TANGGALSCAN, PERIODEOUTPUT, ASSYPLAN) as outputOT1S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(outputOT1S2.TANGGALSCAN,outputOT1S2.ASSYPLAN) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,sum(J1S2) AS J1S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ1S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ1S2.PLANDATE,planJ1S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J2S2) AS J2S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ2S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ2S2.PLANDATE,planJ2S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J3S2) AS J3S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ3S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ3S2.PLANDATE,planJ3S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J4S2) AS J4S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ4S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ4S2.PLANDATE,planJ4S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J5S2) AS J5S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ5S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ5S2.PLANDATE,planJ5S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J6S2) AS J6S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ6S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ6S2.PLANDATE,planJ6S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J7S2) AS J7S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ7S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ7S2.PLANDATE,planJ7S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(J8S2) AS J8S2 FROM plnprod group by PLANDATE, ASSYNO) AS planJ8S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planJ8S2.PLANDATE,planJ8S2.ASSYNO) "
             "LEFT JOIN(SELECT PLANDATE, ASSYNO,SUM(OT1S2) AS OT1S2 FROM plnprod group by PLANDATE, ASSYNO) AS planOT1S2 "
             "on concat(outputqa.TANGGALSCAN,outputqa.ASSYPLAN) = concat(planOT1S2.PLANDATE,planOT1S2.ASSYNO) "
             "WHERE outputqa.TANGGALSCAN ='" + dateyesterday + "' order by outputqa.TANGGALSCAN, outputqa.ASSYPLAN"
             )
    df = pd.read_sql(query,con=dbConnection)
    
    return(df)

def ChangeDatatype(dataframe,cv):
    conveyor = cv
    df = dataframe
    
    df['planJ1S1'] = df['planJ1S1'].astype('int64')
    df['planJ2S1'] = df['planJ2S1'].astype('int64')
    df['planJ3S1'] = df['planJ3S1'].astype('int64')
    df['planJ4S1'] = df['planJ4S1'].astype('int64')
    df['planJ5S1'] = df['planJ5S1'].astype('int64')
    df['planJ6S1'] = df['planJ6S1'].astype('int64')
    df['planJ7S1'] = df['planJ7S1'].astype('int64')
    df['planJ8S1'] = df['planJ8S1'].astype('int64')
    df['planOT1S1'] = df['planOT1S1'].astype('int64')
    df['planOT2S1'] = df['planOT2S1'].astype('int64')

    df['planJ1S2'] = df['planJ1S2'].astype('int64')
    df['planJ2S2'] = df['planJ2S2'].astype('int64')
    df['planJ3S2'] = df['planJ3S2'].astype('int64')
    df['planJ4S2'] = df['planJ4S2'].astype('int64')
    df['planJ5S2'] = df['planJ5S2'].astype('int64')
    df['planJ6S2'] = df['planJ6S2'].astype('int64')
    df['planJ7S2'] = df['planJ7S2'].astype('int64')
    df['planJ8S2'] = df['planJ8S2'].astype('int64')
    df['planOT1S2'] = df['planOT1S2'].astype('int64')

    df['conveyor'] = conveyor 
    return(df)

def pingclient(ipclient):
    
    ping_parser = pingparsing.PingParsing()
    transmitter = pingparsing.PingTransmitter()
    transmitter.destination = ipclient
    transmitter.count = 3
    result = transmitter.ping()

    x = ping_parser.parse(result).as_dict()

    packet_transmit = x["packet_transmit"]
    packet_receive = x["packet_receive"]

    print("\t packet transmit : " + str(packet_transmit))
    print("\t packet receive  : " + str(packet_receive))       

    if packet_transmit == None:
        rate_transmit = 0
    elif packet_transmit != None:
        rate_transmit = round((packet_receive/packet_transmit)*100,2)

    return(rate_transmit)

def InsertTemp(dataframe,username,password,server,database,port):

    df = dataframe
    
    server = server 
    username = username 
    password = password
    database = database
    port = port 

    sqlEngine = create_engine('mysql+pymysql://' + username + ':' + password + '@' + server + '/' + database , pool_recycle=port)
    dbConnection = sqlEngine.connect()
    
    df.to_sql(name='tempoutputqa', con=dbConnection, index=False, if_exists='replace')


def TempToOutput(username,password,server,database,port):

    server = server 
    username = username 
    password = password
    database = database
    port = port 

    sqlEngine = create_engine('mysql+pymysql://' + username + ':' + password + '@' + server + '/' + database , pool_recycle=port)
    
    sql = sqlEngine.execute(
    'insert into outputqa(tanggalscan,conveyor,assyplan,planJ1S1,planJ2S1,planJ3S1,planJ4S1,planJ5S1,planJ6S1,planJ7S1,planJ8S1,planOT1S1,planOT2S1 '
    ',planJ1S2,planJ2S2,planJ3S2,planJ4S2,planJ5S2,planJ6S2,planJ7S2,planJ8S2,planOT1S2 '
    ',outputJ1S1,outputJ2S1,outputJ3S1,outputJ4S1,outputJ5S1,outputJ6S1,outputJ7S1,outputJ8S1,outputOT1S1,outputOT2S1 '
    ',outputJ1S2,outputJ2S2,outputJ3S2,outputJ4S2,outputJ5S2,outputJ6S2,outputJ7S2,outputJ8S2,outputOT1S2) '
    'select tanggalscan,conveyor,assyplan,planJ1S1,planJ2S1,planJ3S1,planJ4S1,planJ5S1,planJ6S1,planJ7S1,planJ8S1,planOT1S1,planOT2S1 '
    ',planJ1S2,planJ2S2,planJ3S2,planJ4S2,planJ5S2,planJ6S2,planJ7S2,planJ8S2,planOT1S2 '
    ',outputJ1S1,outputJ2S1,outputJ3S1,outputJ4S1,outputJ5S1,outputJ6S1,outputJ7S1,outputJ8S1,outputOT1S1,outputOT2S1 '
    ',outputJ1S2,outputJ2S2,outputJ3S2,outputJ4S2,outputJ5S2,outputJ6S2,outputJ7S2,outputJ8S2,outputOT1S2 '
    'FROM tempoutputqa AS temp where concat(temp.tanggalscan,temp.assyplan) not in (select concat(outputqa.tanggalscan,outputqa.assyplan) from outputqa)'
    )
    
    print("\t \t Row Added = " + str(sql.rowcount) + " \n")


def UpdateOutput(username,password,server,database,port):

    server = server 
    username = username 
    password = password
    database = database
    port = port 

    sqlEngine = create_engine('mysql+pymysql://' + username + ':' + password + '@' + server + '/' + database , pool_recycle=port)
    
    sql = sqlEngine.execute(
    'update outputqa set tanggalscan,conveyor,assyplan,planJ1S1,planJ2S1,planJ3S1,planJ4S1,planJ5S1,planJ6S1,planJ7S1,planJ8S1,planOT1S1,planOT2S1 '
    ',planJ1S2,planJ2S2,planJ3S2,planJ4S2,planJ5S2,planJ6S2,planJ7S2,planJ8S2,planOT1S2 '
    ',outputJ1S1,outputJ2S1,outputJ3S1,outputJ4S1,outputJ5S1,outputJ6S1,outputJ7S1,outputJ8S1,outputOT1S1,outputOT2S1 '
    ',outputJ1S2,outputJ2S2,outputJ3S2,outputJ4S2,outputJ5S2,outputJ6S2,outputJ7S2,outputJ8S2,outputOT1S2) '
    'select tanggalscan,conveyor,assyplan,planJ1S1,planJ2S1,planJ3S1,planJ4S1,planJ5S1,planJ6S1,planJ7S1,planJ8S1,planOT1S1,planOT2S1 '
    ',planJ1S2,planJ2S2,planJ3S2,planJ4S2,planJ5S2,planJ6S2,planJ7S2,planJ8S2,planOT1S2 '
    ',outputJ1S1,outputJ2S1,outputJ3S1,outputJ4S1,outputJ5S1,outputJ6S1,outputJ7S1,outputJ8S1,outputOT1S1,outputOT2S1 '
    ',outputJ1S2,outputJ2S2,outputJ3S2,outputJ4S2,outputJ5S2,outputJ6S2,outputJ7S2,outputJ8S2,outputOT1S2 '
    'FROM tempoutputqa AS temp where concat(temp.tanggalscan,temp.assyplan) not in (select concat(outputqa.tanggalscan,outputqa.assyplan) from outputqa)'
    )
    
    print("\t \t Row Updated = " + str(sql.rowcount) + " \n")

if __name__ == '__main__':
    
    print("00. Sever Configuration")

    df_config_server = getConfigServer()
##    datenow = datetime.today().strftime('%Y-%m-%d')
##    dateyesterday = '2022-05-12'
    dateyesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
    n = datetime.now()
    
    print("\t Connected To Server")
    print("\t Processing Date : " + str(dateyesterday))
    print("\t Processing Time : " + str(n.hour)+":"+str(n.minute))
    
    username = df_config_server['username'][0]
    password = df_config_server['pwd'][0]
    server = df_config_server['svr'][0]
    database = df_config_server['db'][0]
    port = df_config_server['portsvr'][0]

    print("\t username \t : " + username)
    print("\t password \t : ********")
    print("\t server   \t : " + server)
    print("\t database \t : " + database)
    print("\t port     \t : " + str(port))
    print("00. Sever Configuration Finished \n")

    print ("01. Get listing client")
    df_master_cv = getListClient(username,password,server,database,port)
    print ("01. Finished \n")
      
    print ("02. Processing client")
    for i in range (len(df_master_cv)) :

        try:
            print ( "Processing seq : " + str(df_master_cv['seq'][i]) + " Conveyor : " +df_master_cv['CV'][i] + " IP : " + df_master_cv['IPServerCV'][i])
            
            rate_transmit = pingclient(df_master_cv['IPServerCV'][i])
            
            print ("\t rate transmit   : " + str(rate_transmit) + " %")

            if rate_transmit >= 30:

                print ("\t Processing GetData from client")
                df_client = getDataClient(df_master_cv['IPServerCV'][i],df_master_cv['databasename'][i])
                print("\t Change Data Type And Add CV Name")
                df_clientChange = ChangeDatatype(df_client,df_master_cv['CV'][i])
                print("\t Insert To Temp")
                InsertTemp(df_clientChange,username,password,server,database,port)
                print("\t Temp to outputqa")
                TempToOutput(username,password,server,database,port)
                
            else:
                
                print ("\t Cannot Processing data (rate transmit < 30%)")
                print ("\t IP Conveyor : " + str(df_master_cv['IPServerCV'][i]))
                print ("\t Conveyor : " + str(df_master_cv['CV'][i]))
                print ("\t Date Access : " + str(dateyesterday))

                Tanggal = str(dateyesterday)
                Jam = str(n.hour)+":"+str(n.minute)
                Seq = i
                CV = str(df_master_cv['CV'][i])
                IP = str(df_master_cv['IPServerCV'][i])
                
                logs = "{},{},{},{},{}\n".format(Tanggal, Jam, Seq, CV, IP)
                file_log = open("errorlog.txt", "a")
                file_log.write(logs)
                file_log.close()
                
        except:
            i = i + 1
            print ("Processing seq :" ,i, "FAILED" + " \n")

            Tanggal = str(dateyesterday)
            Jam = str(n.hour)+":"+str(n.minute)
            Seq = i
            CV = str(df_master_cv['CV'][i])
            IP = str(df_master_cv['IPServerCV'][i])
            
            logs = "{},{},{},{},{}\n".format(Tanggal, Jam, Seq, CV, IP)
            file_log = open("errorlog.txt", "a")
            file_log.write(logs)
            file_log.close()
            
    print ("02. Finished \n")
    n = datetime.now()
    print("Finished Time : " + str(n.hour)+":"+str(n.minute))
