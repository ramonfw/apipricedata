# Configurar entorno segu la página   https://fastapi.tiangolo.com/es/tutorial/
# Editor utilizado: Visual Studio Code

# Visualizador de Base de datos:  DB Browser for SQLite

from typing import Union, List
from fastapi import FastAPI
from fastapi import Request
from starlette.responses import JSONResponse, Response

from starlette.datastructures import MutableHeaders
from starlette.datastructures import Headers

from fastapi import Query
from fastapi import HTTPException
from fastapi import Form

# for proccessing data
import pandas as pd
import numpy as np

# for persists data
import sqlite3

# for users control
import users

# for obtain forex and stock market data
from pandas_datareader import data as pdr
import yfinance as yf

# for processing datetime fields
import time
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
async def verify_token(request: Request, call_next):
    my_header = request.headers.get('X-Token')
#    print("my_header ",my_header)

    pUserAccess = users.UserControl

    resultado = pUserAccess.create_user_data_struct("yFinance.db")

    if (resultado["result"]=="False" and resultado["data"]["msg"] in ("DatabaseError Except","DatabaseError NoExec" )):
        return JSONResponse(content={
            "message": resultado["data"]["msg"] + ": Error de creación o acceso a SQLite database"
        }, status_code=401)

#    if my_header == "customEncriptedToken":
    if my_header != None:

        resultado = pUserAccess.check_token("yFinance.db", my_header)

#        print(resultado)
#        print(resultado["data"]["username"])

        if resultado["result"] == "False":
            return JSONResponse(content={
                "message": "Token incorrect (or not set)."
            }, status_code=401)
        else:
#            request["username"] = resultado["data"]["username"]

            new_header = MutableHeaders(request._headers)
            new_header["x-username"] = resultado["data"]["username"]
            new_header["x-userid"] = resultado["data"]["user_id"]
            request._headers = new_header

            request.scope.update(headers=request.headers.raw)

            response = await call_next(request)
            return response
    else:
#        print(request.query_params)
#        print(request.url)
        actual_url = str(request.url)

        pos = actual_url.find("/docs")
        pos2 = actual_url.find("/openapi.json")
        pos3 = actual_url.find("/users/signin")
        pos4 = actual_url.find("/users/login")

        pos5 = 0
        if actual_url == str(request.base_url):
            pos5 = 1

        if pos>0 or pos2>0 or pos3>0 or pos4>0 or pos5>0:
            response = await call_next(request)
            return response

        return JSONResponse(content={
            "message": "Token not set (or incorrect)."
        }, status_code=401)


@app.get('/')
async def root(request: Request, x_token: Union[List[str], None] = Header(default=None)):
    my_header_token = request.headers.get('x-token')
    my_username = request.headers.get('x-username')
    my_userid = request.headers.get('x-userid')

#    print("request.header2: ", request.headers)

#    if x_token == "customEncriptedToken":
    if x_token != None:
        return {"message": "Wellcome to API for accessing Yahoo Finance Data1","my_header_token": my_header_token,"X-Token": x_token,"username": my_username,"userid": my_userid}
#    elif my_header_token == "customEncriptedToken":
    elif my_header_token != None:
        return {"message": "Wellcome to API for accessing Yahoo Finance Data2","my_header_token": my_header_token,"X-Token": x_token,"username": my_username,"userid": my_userid}
    else:
        return {"message": "Wellcome to API for accessing Yahoo Finance Data3","my_header_token": "No my_header_token, No X-Token, No username, No userid"}


#---  URL:  http://127.0.0.1:8000/market-data/yhfin/?q=TSLA
@app.get("/market-data/yhfin/")
async def read_market_data_yhfin(symbol: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalName="1h", fmt: FmtName=FmtName.json, dsave: GuardarSN="no" ):

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

    results = {"items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt, "save": dsave}]}
    if symbol:
        #download dataframe
        data = pdr.get_data_yahoo(symbol, start=fstart, end=fend, interval=tinterval)
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
            q2 = symbol.replace("=","_")
            pDbAccess = DataPersist
            pDbAccess.dataframe_to_dbtable("yFinance.db", q2, interv, data)

        if fmt!="json":
            data_return = data.to_dict()

        results.update({"symbol": symbol,"total":total_lineas, "data":data_return})
    return results


#---  URL:  http://127.0.0.1:8000/market-data/db/?q=TSLA
@app.get("/market-data/db/")
async def read_market_data_db(symbol: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalName="1h", fmt: FmtName=FmtName.json ):

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

    results = {"items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt}]}
    if symbol:

        interv=tinterval.value
        pDbAccess = DataPersist
        q2 = symbol.replace("=","_")
    
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

        results.update({"symbol": symbol,"total":len_data, "data":data_return})
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
async def market_data_product_info_db(symbol: str="TSLA", fmt: FmtName=FmtName.json ):

    results = {"items": [{"d": symbol, "format": fmt}]}
    if symbol:

        pDbAccess = DataPersist
        data = pDbAccess.get_products_info("yFinance.db",symbol)

        if len(data) > 0:
            data_return = data.to_dict('records')
            len_data = len(data)

            if fmt!="json":
                data_return = data.to_dict()
        else:
            len_data = 0
            data_return = {"tabla": " No encontrada"}

        results.update({"symbol": symbol,"rango_fechas":data_return})
    return results


@app.get("/users/me")
async def read_user_me(request: Request): 
#    my_header = request.headers.get('x-token')
    my_username = request.headers.get('x-username')
    my_userid = request.headers.get('x-userid')

#    print("request.header2: ", request.headers)

    return {"username": my_username,"userid": my_userid}


@app.get("/users/{user_id}")
async def read_user(user_id: str):

    pUserAccess = users.UserControl
    resultado = pUserAccess.get_user_by_id("yFinance.db", user_id)

    return {"resultado": resultado}


@app.post("/users/signin/")
async def login_user(username: str = Form(), password: str = Form()):
    pUserAccess = users.UserControl

    resultado = pUserAccess.user_signin("yFinance.db", username, password)

    return {"resultado": resultado}


@app.post("/users/login/")
async def login_user(username: str = Form(), password: str = Form(), ClientKey: str = Form(), ClientSecret: str = Form()):
    pUserAccess = users.UserControl
    resultado = pUserAccess.user_login("yFinance.db", username, password, ClientKey, ClientSecret)

    return {"username": resultado}


#--- Get Header ---
@app.get("/items_header/")
async def read_items_header(request: Request, x_token: Union[List[str], None] = Header(default=None)):
#    my_header = request.headers.get('X-Token')
#    if my_header == "customEncriptedToken":
#        return {"X-Token": my_header, "otroToken": x_token}

#    if str(x_token) == "customEncriptedToken":
    if x_token != None:
        return {"tokenRequestHeader": x_token}
    
    return {"tokenRequestHeader": "No token"}
