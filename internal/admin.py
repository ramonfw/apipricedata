# for persists data
import sqlite3

# for proccessing data
import pandas as pd

import time
from datetime import datetime
from datetime import timedelta

import hashlib

from fastapi import APIRouter, Depends, HTTPException

# for users control
import usersController
import logController

#--- para  parametros ENUM ---
from enum import Enum

class ScopeEnum(str, Enum):
    all = "ALL"
    login = "LOGIN"
    signup = "SIGNUP"

class RolEnum(str, Enum):
    sadm = "SADM"  #superadmin
    adm = "ADM"  #admin
    usr = "USR"  #usuario
    inv = "INV"  #invitado



str_header_admin = ". <br>(<b>Needs a <u>user login token</u> in the <u>x-token header param</u></b>)"

tags_metadata_admin = [
    {
        "name": "read_all_user_list",
        "description": "Get a list of all the users registered"+str_header_admin,
    },
    {
        "name": "read_user_id",
        "description": "Get a user data for given user_id"+str_header_admin,
    },
    {
        "name": "read_logins_id",
        "description": "Get logins list for a given user_id"+str_header_admin,
    },
    {
        "name": "read_logins_date",
        "description": "Get logins list for a given date range"+str_header_admin,
    },
    {
        "name": "read_id_requests",
        "description": "Get API requests list for a given user_id"+str_header_admin,
    },
    {
        "name": "read_date_requests",
        "description": "Get API requests list for a given date range"+str_header_admin,
    },

]


router = APIRouter(
    prefix="/admin",
#    tags=["admin"],
    responses={404: {"description": "Not found"}},
#    openapi_tags=tags_metadata_admin,
)



#--USERS LIST ---
@router.get("/users/list/all/", tags=["read_all_user_list"])
async def read_all_user_list():
    pUserAccess = usersController.UserControl

    vDatos = pUserAccess.get_user_list("yFinance.db",0)
    return {"data":vDatos}


@router.get("/users/list/{user_id}", tags=["read_user_id"])
async def read_user_id(user_id: int):
    pUserAccess = usersController.UserControl

    vDatos = pUserAccess.get_user_list("yFinance.db",user_id)
    return {"data":vDatos}



#--USERS LOGINS ---
@router.get("/users/logins/id/", tags=["read_logins_id"])
async def read_logins_id(user_id: int, scope:ScopeEnum = ScopeEnum.all):
    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_user_logins("yFinance.db", user_id, scope)
    return {"data":vDatos}



@router.get("/users/logins/date/", tags=["read_logins_date"])
async def read_logins_date(fstart: str="-30", fend: str="today", scope:ScopeEnum = ScopeEnum.all):

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

#        def get_date_logins(pDataBase, pFIni, pFEnd, pScope):
    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_date_logins("yFinance.db", fstart, fend, scope)
    return {"data":vDatos}


#-- REQUESTS LIST ---
@router.get("/request/id/", tags=["read_id_requests"])
async def read_id_requests(user_id: int):
    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_id_requests("yFinance.db", user_id)
    return {"data":vDatos}


@router.get("/request/date/", tags=["read_date_requests"])
async def read_date_requests(fstart: str="-30", fend: str="today"):

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

    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_date_requests("yFinance.db", fstart, fend)
    return {"data":vDatos}


@router.put("/rol/{user_id}",
    tags=["update_user_rol"],
    responses={403: {"description": "Operation forbidden"}},
)
async def update_user_rol(user_id: str, user_rol:RolEnum=RolEnum.inv):


#    if item_id != "plumbus":
#        raise HTTPException(
#            status_code=403, detail="You can only update the item: plumbus"
#        )


    return {"userid": user_id, "name": "The great Plumbus"}
