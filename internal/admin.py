# for persists data
import sqlite3

# for proccessing data
import pandas as pd

import time
from datetime import datetime
from datetime import timedelta

import hashlib

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Request


# for users control
from controllers import usersController
from controllers import logController

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



str_header_admin = ". <br>(<b>Needs a <u><i>user login token</i></u> in the <u><i>x-token header param</i></u></b>)"

tags_metadata_admin = [
    {
        "name": "Read_all_user_list",
        "description": "Get a list of all the users registered"+str_header_admin,
    },
    {
        "name": "Read_user_data",
        "description": "Get a user data for given user_id"+str_header_admin,
    },
    {
        "name": "Read_logins_for_id",
        "description": "Get logins list for a given user_id"+str_header_admin,
    },
    {
        "name": "Read_logins_in_date_range",
        "description": "Get logins list for a given date range"+str_header_admin,
    },
    {
        "name": "Read_requests_for_id",
        "description": "Get API requests list for a given user_id"+str_header_admin,
    },
    {
        "name": "Read_requests_in_date_range",
        "description": "Get API requests list for a given date range"+str_header_admin,
    },
    {
        "name": "Update_user_rol",
        "description": "Update user rol for a given userId, for more access level to user and logs data"+str_header_admin,
    },

]


router = APIRouter(
    prefix="/admin",
    responses={404: {"description": "Not found"}},
)



#-- USERS LIST ---

@router.get("/users/list/all/", tags=["Read_all_user_list"])
async def read_all_user_list(request: Request):
    vUserRol = request.headers.get('x-userrol')
    pUserAccess = usersController.UserControl

    vDatos = pUserAccess.get_user_list("yFinance.db", 0, vUserRol)
    return vDatos


@router.get("/users/list/{user_id}", tags=["Read_user_data"])
async def read_user_id(request: Request, user_id: int):
    vUserRol = request.headers.get('x-userrol')
    pUserAccess = usersController.UserControl

    vDatos = pUserAccess.get_user_list("yFinance.db",user_id, vUserRol)
    return {"data":vDatos}



#-- USERS LOGINS LISTS---

@router.get("/users/logins/id/", tags=["Read_logins_for_id"])
async def read_logins_id(user_id: int, scope:ScopeEnum = ScopeEnum.all):
    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_user_logins("yFinance.db", user_id, scope)
    return {"data":vDatos}



@router.get("/users/logins/date/", tags=["Read_logins_in_date_range"])
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

    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_date_logins("yFinance.db", fstart, fend, scope)
    return {"data":vDatos}


#-- REQUESTS LIST ---

@router.get("/requests/id/", tags=["Read_requests_for_id"])
async def read_id_requests(user_id: int):
    pLogAccess = logController.LogControl

    vDatos = pLogAccess.get_id_requests("yFinance.db", user_id)
    return {"data":vDatos}


@router.get("/requests/date/", tags=["Read_requests_in_date_range"])
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


#-- EDIT ROLS ---

@router.put("/rol/{user_id}",
    tags=["Update_user_rol"],
    responses={403: {"description": "Operation forbidden"}},
)
async def update_user_rol(request: Request, user_id: str, user_rol:RolEnum=RolEnum.inv):
    userid_admin = request.headers.get('x-userid')
    userrol_admin = request.headers.get('x-userrol')

    if userrol_admin not in (RolEnum.sadm,RolEnum.adm):
        vDatos ={"result":"False", "msg":"Usuario no tiene privilegios para cambio de roles"}
    elif userrol_admin != RolEnum.sadm and user_rol in (RolEnum.sadm ,RolEnum.adm ,RolEnum.usr):
        vDatos ={"result":"False", "msg":"Usuario no tiene suficientes privilegios para cambiar el rol a "+user_rol}
    else:
        pUserAccess = usersController.UserControl
        vDatos = pUserAccess.user_change_rol("yFinance.db", user_id, user_rol)

    return vDatos
