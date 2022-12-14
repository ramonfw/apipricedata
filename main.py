# Configurar entorno segu la página   https://fastapi.tiangolo.com/es/tutorial/
# Editor utilizado: Visual Studio Code
# Visualizador de Base de datos:  DB Browser for SQLite

from fastapi import APIRouter

from internal import admin
#from .routers import items, users

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

# for users control and logs control
from controllers import usersController
from controllers import logController

# for obtain forex and stock market data
from pandas_datareader import data as pdr
import yfinance as yf

# for processing datetime fields
import time
from datetime import datetime
from datetime import timedelta

#--- para  parametros ENUM ---
from enum import Enum

class ModelEnum(str, Enum):
    alexnet = "alexnet"
    resnet = "resnet"
    lenet = "lenet"

class FmtEnum(str, Enum):
    json = "json"
    dict = "dict"

class IntervalEnum(str, Enum):
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

class GuardarEnum(str, Enum):
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


str_header = ". <br>(<b>Needs a <u><i>user login token</i></u> in the <u><i>x-token header param</i></u></b>)"

tags_metadata = [
    {
        "name": "Root",
        "description": "Permit test if API is working, receiving a wellcome response.",
    },
    {
        "name": "Read_market_data_Yahoo_Finance",
        "description": "Download data from Yahoo finance for Symbol, data range and time frame passed as parameters. Response is in selected format, and data isa saved/not saved in SQLite database"+str_header,
    },
    {
        "name": "Read_market_data_SQLite_DB",
        "description": "Get data from SQLite database for Symbol, data range and time frame passed as parameters. Response is in selected format"+str_header,
    },
    {
        "name": "Market_data_product_list_SQLite_DB",
        "description": "Get a list of registered Symbols saved in SQLite database"+str_header,
    },
    {
        "name": "Market_data_product_info_SQLite_DB",
        "description": "Get info about a Symbol/product saved in SQLite database"+str_header,
    },
    {
        "name": "Read_user_current",
        "description": "Get data about current user (ME)"+str_header,
    },
    {
        "name": "Read_user_by_id",
        "description": "Get data about the user passed as parameter"+str_header,
    },
    {
        "name": "Signup_user",
        "description": "Sign Up the user with a given username and password, passed as parameters.<br>Response includes the ClientKey and ClientSecret, needed for login.",
    },
    {
        "name": "Login_user",
        "description": "Login user with a given username, password, ClientKey and ClientSecret, passed as parameters",
    },
    {
        "name": "read_items_header",
        "description": "Endpoint included for monitoring"+str_header,
    },
    
]

app = FastAPI(
    title="API for downloading and show Yahoo finance data",
    description="API permit download and query stocks data from Yahoo Finance, and is a security access implementation test.",
    version="1.1.2",
    openapi_tags=tags_metadata+admin.tags_metadata_admin,
)

app.include_router(admin.router)

@app.middleware("http")
async def verify_token(request: Request, call_next):
    my_header_token = request.headers.get('X-Token')

#    my_client_ip = request.environ.get('HTTP_X_FORWARDED_FOR') or request.environ.get('REMOTE_ADDR')
    my_client_ip = request.client.host
#    print ("Client IP: ",my_client_ip)

    xf = 'x-forwarded-for'.encode('utf-8')
    xh = 'host'.encode('utf-8')

    forward_ip = "0:0:0:0"
    origin_ip = "0:0:0:0"

    for header in request.headers.raw:
        if header[0] == xf:
            print("Find out the forwarded-for ip address")
            forward_ip = header[1].decode('utf-8')
        if header[0] == xh:
            print("Find out the host ip address")
            origin_ip = header[1].decode('utf-8')

#        print(f"forward_ip:\t{forward_ip}")
#        print(f"origin_ip:\t{origin_ip}")

    params = request.query_params

    pUserAccess = usersController.UserControl
    resultado = pUserAccess.create_user_data_struct("yFinance.db")

    if (resultado["result"]=="False" and resultado["message"] in ("DatabaseError Except","DatabaseError NoExec" )):
        return JSONResponse(content={
            "result":"False","message": resultado["message"] + ": Error de creación o acceso a SQLite database"
        }, status_code=401)

    actual_url = str(request.url)

    if my_header_token != None:
        resultado = pUserAccess.check_token("yFinance.db", my_header_token)

        vUserId = resultado["data"]["user_id"]
        vUserRol = resultado["data"]["rol"]
#        print("resultado ",resultado)

        pApiRequestRegistry = logController.LogControl
        respuesta = pApiRequestRegistry.save_api_request("yFinance.db", vUserId, request.url, my_client_ip, str(params), my_header_token)

        pos5 = actual_url.find("/admin")
        if pos5>0 and vUserRol != "ADM" and vUserRol != "SADM":
            return JSONResponse(content={
                "result":"False","message": "Usuario "+vUserId + " no tiene suficientes privilegios"
            }, status_code=401)

        if resultado["result"] == "False":
            return JSONResponse(content={
                "result":"False","message": resultado["message"]   #"Token incorrect (or not set)."
            }, status_code=401)
        else:
            new_header = MutableHeaders(request._headers)
            new_header["x-username"] = resultado["data"]["username"]
            new_header["x-userid"] = vUserId
            new_header["x-userrol"] = vUserRol
            new_header["x-cust-txt-response"] = respuesta["message"]
            new_header["x-client-ip"] = my_client_ip
            request._headers = new_header

            request.scope.update(headers=request.headers.raw)

            response = await call_next(request)
            return response
    else:
        pos = actual_url.find("/docs")
        pos2 = actual_url.find("/openapi.json")
        pos3 = actual_url.find("/users/signup/")
        pos4 = actual_url.find("/users/login/")

        pos5 = actual_url.find("/admin")
#-- quitar lo siguiente para testear prefix /admin
        pos5 = 0

        vUserId = -1  
        pApiRequestRegistry = logController.LogControl
        respuesta = pApiRequestRegistry.save_api_request("yFinance.db", vUserId, request.url, "0:0:0:0", str(params), "None")

        pos10 = 0
        if actual_url == str(request.base_url):
            pos10 = 1

        if pos>0 or pos2>0 or pos3>0 or pos4>0 or pos5>0 or pos10>0:
            response = await call_next(request)
            return response

        return JSONResponse(content={
            "result":"False","message": "Token not set (or incorrect)."
        }, status_code=401)


@app.get('/', tags=["Root"] )
async def root(request: Request, x_token: Union[List[str], None] = Header(default=None)):
    my_header_token = request.headers.get('x-token')
    my_username = request.headers.get('x-username')
    my_userid = request.headers.get('x-userid')
    my_client_ip = request.headers.get('x-client-ip')

    if my_client_ip == None:
        my_client_ip = "0:0:0:0"

    str_header = ""
    for header in request.headers.raw:
        str_header += header[0].decode('utf-8')+":"+header[1].decode('utf-8')+"<br>"

    str_header = "Processed Ok"    

    if x_token != None:
        return {"result":"True","message": "Wellcome to API for accessing Yahoo Finance Data1. Client IP: "+my_client_ip,"str_header": str_header,"my_header_token": my_header_token,"X-Token": x_token,"username": my_username,"userid": my_userid}
    elif my_header_token != None:
        return {"result":"True","message": "Wellcome to API for accessing Yahoo Finance Data2. Client IP: "+my_client_ip,"str_header": str_header,"my_header_token": my_header_token,"X-Token": x_token,"username": my_username,"userid": my_userid}
    else:
        return {"result":"False","message": "Wellcome to API for accessing Yahoo Finance Data3. Client IP: "+my_client_ip,"str_header": str_header,"my_header_token": "No my_header_token, No X-Token, No username, No userid"}


#---  URL:  http://127.0.0.1:8000/market-data/yhfin/?q=TSLA
@app.post("/market-data/yhfin/", tags=["Read_market_data_Yahoo_Finance"] )
async def read_market_data_yhfin(symbol: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalEnum="1h", fmt: FmtEnum=FmtEnum.json, dsave: GuardarEnum=GuardarEnum.si):

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

    results = {"result":"False","message":"Data NOT retrieved","items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt, "save": dsave}]}
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

        results = {"result":"True","message":"Data retrieved","items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt, "save": dsave}],"symbol": symbol,"total":total_lineas, "data":data_return}
    return results


#---  URL:  http://127.0.0.1:8000/market-data/db/?q=TSLA
@app.get("/market-data/db/", tags=["Read_market_data_SQLite_DB"])
async def read_market_data_db(symbol: str="TSLA", fstart: str="-30", fend: str="today", tinterval: IntervalEnum="1h", fmt: FmtEnum=FmtEnum.json ):

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

    results = {"result":"False","message":"Data NOT retrieved","items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt}]}
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

        results = {"result":"True","message":"Data retrieved","items": [{"d": symbol, "start": fstart, "end": fend, "interval": tinterval, "format": fmt}],"symbol": symbol,"total":len_data, "data":data_return}
    return results


#---  URL:  http://127.0.0.1:8000/market-data/list-products-db/
@app.get("/market-data/products-list-db/", tags=["Market_data_product_list_SQLite_DB"] )
async def market_data_productList_db(db: str="yFinance.db", fmt: FmtEnum=FmtEnum.json ):

    results = {"result":"False","message":"Data NOT retrieved","items": [{"db": db, "format": fmt}]}
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

        results={"result":"True","message":"Data retrieved","items": [{"db": db, "format": fmt}],"total":len_data, "products":data_return}
    return results


#---  URL:  http://127.0.0.1:8000/market-data/list-products-db/
@app.get("/market-data/product-info-db/", tags=["Market_data_product_info_SQLite_DB"])
async def market_data_product_info_db(symbol: str="TSLA", fmt: FmtEnum=FmtEnum.json ):

    results = {"result":"False","message":"Data NOT retrieved","items": [{"d": symbol, "format": fmt}]}
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

        results = {"result":"True","message":"Data retrieved","items": [{"d": symbol, "format": fmt}],"symbol": symbol,"rango_fechas":data_return}
    return results


@app.get("/users/me", tags=["Read_user_current"])
async def read_user_me(request: Request): 
    my_username = request.headers.get('x-username')
    my_userid = request.headers.get('x-userid')

    datos={"username": my_username,"userid": my_userid}

    resu="True"
    message="Current user found"
    
    if my_userid==None:
        resu="False"
        message="Current user NOT found"

    return {"result": resu, "message": message, "data":datos}


@app.get("/users/{user_id}", tags=["Read_user_by_id"])
async def read_user(user_id: str):

    pUserAccess = usersController.UserControl
    resultado = pUserAccess.get_user_by_id("yFinance.db", user_id)

    return resultado


@app.post("/users/signup/", tags=["Signup_user"])
async def signup_user(username: str = Form(), password: str = Form()):
    pUserAccess = usersController.UserControl

    resultado = pUserAccess.user_signup("yFinance.db", username, password)

    if resultado["result"] == "True":
        vUserId = resultado["data"]["userid"]
        vUsername = resultado["data"]["username"]
        vRol = resultado["data"]["userrol"]
        vToken = resultado["data"]["Token"]
    else:
        vUserId = -1
        vUsername = username
        vRol = "No"
        vToken = "Signup error"

    pApiSignupRegistry = logController.LogControl
    respuesta_signup = pApiSignupRegistry.save_login_request("yFinance.db", vUserId, vUsername, vRol, vToken,"SIGNUP")

    return {"resultado": resultado}


@app.post("/users/login/", tags=["Login_user"])
async def login_user(username: str = Form(), password: str = Form(), ClientKey: str = Form()):
    pUserAccess = usersController.UserControl
    resultado = pUserAccess.user_login("yFinance.db", username, password, ClientKey)

    if resultado["result"] == "True":
        vUserId = resultado["data"]["userid"]
        vUsername = resultado["data"]["username"]
        vRol = resultado["data"]["userrol"]
        vToken = resultado["data"]["Token"]
    else:
        vUserId = -1
        vUsername = username
        vRol = "No"
        vToken = "Login error"

    pApiLoginRegistry = logController.LogControl
    respuesta_login = pApiLoginRegistry.save_login_request("yFinance.db", vUserId, vUsername, vRol, vToken,"LOGIN")

    return resultado


#--- Get Header ---
@app.get("/items_header/", tags=["read_items_header"])
async def read_items_header(request: Request, x_token: Union[List[str], None] = Header(default=None)):
#    my_header = request.headers.get('X-Token')
#    if my_header == "customEncriptedToken":
#        return {"X-Token": my_header, "otroToken": x_token}

#    if str(x_token) == "customEncriptedToken":
    if x_token != None:
        return {"tokenRequestHeader": x_token}
    
    return {"tokenRequestHeader": "No token"}
