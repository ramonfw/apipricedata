# Configurar entorno segu la página   https://fastapi.tiangolo.com/es/tutorial/
# Editor utilizado: Visual Studio Code

# Visualizador de Base de datos:  DB Browser for SQLite

from typing import Union, List
from fastapi import FastAPI
from fastapi import Request
from fastapi import Query
from fastapi import HTTPException

# for proccessing data
import pandas as pd
import numpy as np

import time

# for persists data
import sqlite3

# for obtain forex and stock market data
from pandas_datareader import data as pdr
import yfinance as yf


# for processing datetime fields

from datetime import datetime
from datetime import timedelta


#--- para  parametros ENUM ---
from enum import Enum

class ModelName(str, Enum):
    alexnet = "alexnet"
    resnet = "resnet"
    lenet = "lenet"

class FmtName(str, Enum):
    json = "json"
    dict = "dict"

class IntervalName(str, Enum):
    m1 = "1m"
    m2 = "2m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    m60 = "60m"
    m90 = "90m"
    h1 = "1h"
    d1 = "1d"
    d5 = "5d"
    w1 = "1wk"
    mo1 = "1mo"
    mo3 = "3mo"

class GuardarSN(str, Enum):
    si = "si"
    no = "no"

#--- fin para parametros ENUM ---


#--- para Header  ---
from fastapi import Header
#--- fin para Header ---


#--- para Query params obligatorio ---
from pydantic import Required

#--- para  request BODY ---
from pydantic import BaseModel


class Item(BaseModel):
    name: str
    description: Union[str, None] = None
    price: float
    tax: Union[float, None] = None

#--- fin para Query params ---

#--- clase para acceso a datos SqLite ----
class DataPersist:

    def get_products_list(pDataBase):
        vConn1 = sqlite3.connect(pDataBase)    

        vDfP = {}
        try:  
            vSelect_sql = "SELECT name FROM sqlite_master WHERE type='table'"
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
        except:
            vDfP = None

        return vDfP


    def get_products_info(pDataBase, pProduct):
        vConn1 = sqlite3.connect(pDataBase)    

        vDfP = {}
        try:  
            vSelect_sql = f"SELECT FechaHora FROM {pProduct} ORDER BY FechaHora ASC LIMIT 1"
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)

            vSelect_sql = f"SELECT FechaHora FROM {pProduct} ORDER BY FechaHora DESC LIMIT 1"
            vDf_fin = pd.read_sql_query(vSelect_sql, vConn1)

            vDfP = vDfP.append(vDf_fin, ignore_index = True)

        except:
            vDfP = None

        return vDfP



    def dataframe_to_dbtable(pDataBase, pTableName, pTFrame, pDfP):
        vConn = sqlite3.connect(pDataBase)

        ToSQL = False
        if ToSQL:
            pDfP.to_sql(name=pTableName, con=vConn, if_exists="replace", index=False)
            vConn.commit()
        else:
            cursor = vConn.cursor()

            list_products = DataPersist.get_products_list(pDataBase)
            product_found = False

            for i in range(len(list_products.name)):
                if list_products.name[i] == pTableName:
                    product_found = True
#                    print("Producto ",i,list_products.name[i])
            
            create_sql = "CREATE TABLE IF NOT EXISTS "+pTableName+" (FechaHora DATETIME, Open FLOAT, Close FLOAT, High FLOAT, Low FLOAT, Adj_Close FLOAT, Volume LONG, TFrame TEXT)"
            cursor.execute(create_sql)

            if product_found == False:
                index_sql = "CREATE INDEX idx"+pTableName+" ON "+pTableName+" (FechaHora, TFrame)"
                cursor.execute(index_sql)

            select_sql = "SELECT FechaHora, TFrame FROM "+pTableName+" WHERE TFrame='"+pTFrame+"' ORDER BY FechaHora DESC"
            dbCursor = cursor.execute(select_sql)

            pFechaHora = '-'
            fila = dbCursor.fetchone()
            if fila != None:
                pFechaHora = fila[0]

            for row in pDfP.itertuples():
                if pFechaHora < str(row[1]):
                    insert_sql = f"INSERT INTO {pTableName} (FechaHora, Open, High, Low, Close, Adj_Close, Volume, TFrame) VALUES ('{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, '{row[8]}')"
                    cursor.execute(insert_sql)
                else:
                    select_sql = "SELECT FechaHora, TFrame FROM "+pTableName+" WHERE FechaHora='"+str(row[1])+"' AND TFrame='"+pTFrame+"' ORDER BY FechaHora DESC"
                    dbCursor = cursor.execute(select_sql)
                    fila2 = dbCursor.fetchone()
                    if fila2 == None:
                        insert_sql = f"INSERT INTO {pTableName} (FechaHora, Open, High, Low, Close, Adj_Close, Volume, TFrame) VALUES ('{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, '{row[8]}')"
                        cursor.execute(insert_sql)

        vConn.commit()
        return True


    def dbtable_to_dataframe(pDataBase, pTableName, pFechaStart, pFechaEnd, pTFrame):
        vConn1 = sqlite3.connect(pDataBase)    

        vDfP = {}
        try:  
            vSelect_sql = "SELECT * FROM "+pTableName+"  WHERE TFrame='"+pTFrame+"' AND FechaHora>='"+pFechaStart+"' AND FechaHora<='"+pFechaEnd+"' "
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
        except:
            vDfP = None

        return vDfP

#--- FIN clase para acceso a datos SqLite ----


app = FastAPI()


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.get('/')
async def root():
    return {"message": "Wellcome to API for accessing Yahoo Finance Data"}


#---  URL:  http://127.0.0.1:8000/market-data/yhfin/?q=TSLA
@app.get("/market-data/yhfin/")
async def read_market_data_yhfin(q: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalName="1h", fmt: FmtName=FmtName.json, dsave: GuardarSN="no" ):

    now = datetime.now()
    if fend=="today":
        fend = now.strftime("%Y-%m-%d")
    elif fend>now.strftime("%Y-%m-%d"):
        fend = now.strftime("%Y-%m-%d")
    else:
        now = datetime.strptime(fend,"%Y-%m-%d") 

    if fstart=="-30":
        fstart= now - timedelta(days=30)
        fstart = fstart.strftime("%Y-%m-%d")
    elif fstart>fend:
        raise HTTPException(status_code=200, detail="fecha inicio("+fstart+") mayor que fecha fin("+fend+")")

    yf.pdr_override()

    results = {"items": [{"d": q, "start": fstart, "end": fend, "interval": tinterval, "format": fmt, "save": dsave}]}
    if q:
        #download dataframe
        data = pdr.get_data_yahoo(q, start=fstart, end=fend, interval=tinterval)
        total_lineas = len(data)

        idx = np.arange(1,len(data)+1,1)
        data["indice"] = idx
        data = data.reset_index()
        data = data.set_index("indice")
        data = data.rename({'index': 'FechaHora'}, axis=1)

        interv=tinterval.value

        tFrame = np.full((total_lineas),interv).tolist()
        data["tframe"] = tFrame
        
        data_return = data.to_dict('records')

        if dsave.value == "si":
            q2 = q.replace("=","_")
            pDbAccess = DataPersist
            pDbAccess.dataframe_to_dbtable("yFinance.db", q2, interv, data)

        if fmt!="json":
            data_return = data.to_dict()

        results.update({"q": q,"total":total_lineas, "data":data_return})
    return results


#---  URL:  http://127.0.0.1:8000/market-data/db/?q=TSLA
@app.get("/market-data/db/")
async def read_market_data_db(q: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalName="1h", fmt: FmtName=FmtName.json ):

    now = datetime.now()
    if fend=="today":
        fend = now.strftime("%Y-%m-%d")
    elif fend>now.strftime("%Y-%m-%d"):
        fend = now.strftime("%Y-%m-%d")
    else:
        now = datetime.strptime(fend,"%Y-%m-%d") 

    if fstart=="-30":
        fstart= now - timedelta(days=30)
        fstart = fstart.strftime("%Y-%m-%d")
    elif fstart>fend:
        raise HTTPException(status_code=200, detail="fecha inicio("+fstart+") mayor que fecha fin("+fend+")")

    yf.pdr_override()

    results = {"items": [{"d": q, "start": fstart, "end": fend, "interval": tinterval, "format": fmt}]}
    if q:

        interv=tinterval.value
        pDbAccess = DataPersist
        q2 = q.replace("=","_")
    
        #load dataframe from SQLite table
        data = pDbAccess.dbtable_to_dataframe("yFinance.db", q2, fstart, fend, interv)

        if data is None:
            len_data = 0
            data_return = {"tabla": q2+" No encontrada"}
        else:
            data_return = data.to_dict('records')
            len_data = len(data)

            if fmt!="json":
                data_return = data.to_dict()

        results.update({"q": q,"total":len_data, "data":data_return})
    return results


#---  URL:  http://127.0.0.1:8000/market-data/list-products-db/
@app.get("/market-data/products-list-db/")
async def market_data_productList_db(db: str="yFinance.db", fmt: FmtName=FmtName.json ):

    results = {"items": [{"db": db, "format": fmt}]}
    if db:

        pDbAccess = DataPersist

        data = pDbAccess.get_products_list(db)

        if len(data) > 0:
            data_return = data.to_dict('records')
            len_data = len(data)

            if fmt!="json":
                data_return = data.to_dict()
        else:
            len_data = 0
            data_return = {"tabla": " No encontrada"}

        results.update({"db": db,"total":len_data, "products":data_return})
    return results


#---  URL:  http://127.0.0.1:8000/market-data/list-products-db/
@app.get("/market-data/product-info-db/")
async def market_data_product_info_db(q: str="TSLA", fmt: FmtName=FmtName.json ):

    results = {"items": [{"d": q, "format": fmt}]}
    if q:

        pDbAccess = DataPersist
        data = pDbAccess.get_products_info("yFinance.db",q)

        if len(data) > 0:
            data_return = data.to_dict('records')
            len_data = len(data)

            if fmt!="json":
                data_return = data.to_dict()
        else:
            len_data = 0
            data_return = {"tabla": " No encontrada"}

        results.update({"d": q,"rango_fechas":data_return})
    return results


@app.get("/users/me")
async def read_user_me():
    return {"user_id": "the current user"}


@app.get("/users/{user_id}")
async def read_user(user_id: str):
    return {"user_id": user_id}


#--- Get Header ---
    
@app.get("/items_header/")
async def read_items_header(x_token: Union[List[str], None] = Header(default=None)):
    return {"X-Token values": x_token}