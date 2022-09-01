# for persists data
import sqlite3

# for proccessing data
import pandas as pd

from datetime import datetime
from datetime import timedelta

import hashlib



#--- clase para acceso a datos SqLite ----
class LogControl:

    def get_user_logins(pDataBase, pUserId, pScope):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        respuesta ={}
        resu = "False"

        try:  
            if pUserId<1:
                if pScope == "ALL":
                    vSelect_sql = "SELECT * FROM users_logins "
                    dbCursor = cursor.execute(vSelect_sql)
                else:
                    vSelect_sql = "SELECT * FROM users_logins where accion = ?  and ? = ?"
                    dbCursor = cursor.execute(vSelect_sql,(pScope,1,1))
            else:
                if pScope == "ALL":
                    vSelect_sql = "SELECT * FROM users_logins where userid = ? and ? = ?"
                    dbCursor = cursor.execute(vSelect_sql,(pUserId,1,1))
                else:
                    vSelect_sql = "SELECT * FROM users_logins where userid = ? and accion = ?"
                    dbCursor = cursor.execute(vSelect_sql,(pUserId,pScope))
                    
            filas = dbCursor.fetchall()

            nombreFilas= [dataNombres[0] for dataNombres in cursor.description]

            datos = []

            for tfila in filas:
                fila = list(tfila)
                
                vDatos = {}
                for i in range(len(fila)):
                    vDatos[nombreFilas[i]] = fila[i]
                datos.append(vDatos)
                resu = "True"
            if resu=="True":  
                msg = "Listado obtenido satisfactoriamente"  
            else:  
                msg = "Listado obtenido vacio, el id " + str(pUserId) + " no tiene logins registrados con accion " + pScope
            respuesta ={"result":resu,"msg":msg,"fieldnames":nombreFilas,"data":datos}
        except Exception as e:   # work on python 3.x
            respuesta ={"result":"false","msg":"Users logins not found due to ERROR -"+ str(e),"data":[]}

        vConn1.close()
        return respuesta



    def get_date_logins(pDataBase, pFIni, pFEnd, pScope):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        respuesta ={}
        resu = "False"

        vFEnd = datetime.strptime(pFEnd,"%Y-%m-%d") 

        vFEnd= vFEnd + timedelta(days=1)
        pFEnd = vFEnd.strftime("%Y-%m-%d")

        try:  
            if pScope == "ALL":
                vSelect_sql = "SELECT * FROM users_logins where FechaHora >= ? and FechaHora <= ?"
                dbCursor = cursor.execute(vSelect_sql,(pFIni, pFEnd))
            else:
                vSelect_sql = "SELECT * FROM users_logins where  FechaHora >= ? and FechaHora <= ? and accion = ?"
                dbCursor = cursor.execute(vSelect_sql,(pFIni, pFEnd,pScope))
                    
            filas = dbCursor.fetchall()

            nombreFilas= [dataNombres[0] for dataNombres in cursor.description]

            datos = []

            for tfila in filas:
                fila = list(tfila)
                
                vDatos = {}
                for i in range(len(fila)):
                    vDatos[nombreFilas[i]] = fila[i]
                datos.append(vDatos)
                resu = "True"
            if resu=="True":  
                msg = "Listado obtenido satisfactoriamente"  
            else:  
                msg = "Listado obtenido vacio, no existen accesos en el rango de fechas con accion " + pScope
            respuesta ={"result":resu,"msg":msg,"FIni":pFIni,"FEnd":pFEnd,"fieldnames":nombreFilas,"data":datos}
        except Exception as e:   # work on python 3.x
            respuesta ={"result":"false","msg":"Users logins not found due to ERROR -"+ str(e),"data":[]}

        vConn1.close()
        return respuesta


    def get_id_requests(pDataBase, pUserId):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        respuesta ={}
        resu = "False"

        try:  
            vSelect_sql = "SELECT * FROM api_requests where userid = ?  and ? = ?"
            dbCursor = cursor.execute(vSelect_sql,(pUserId,1,1))
                    
            filas = dbCursor.fetchall()

            nombreFilas= [dataNombres[0] for dataNombres in cursor.description]

            datos = []

            for tfila in filas:
                fila = list(tfila)
                
                vDatos = {}
                for i in range(len(fila)):
                    vDatos[nombreFilas[i]] = fila[i]
                datos.append(vDatos)
                resu = "True"
            if resu=="True":  
                msg = "Listado obtenido satisfactoriamente"  
            else:  
                msg = "Listado obtenido vacio, el id " + str(pUserId) + " no tiene requests registrados"
            respuesta ={"result":resu,"msg":msg,"fieldnames":nombreFilas,"data":datos}
        except Exception as e:   # work on python 3.x
            respuesta ={"result":"false","msg":"Users requests not found due to ERROR -"+ str(e),"data":[]}

        vConn1.close()
        return respuesta


    def get_date_requests(pDataBase, pFIni, pFEnd):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        respuesta ={}
        resu = "False"

        vFEnd = datetime.strptime(pFEnd,"%Y-%m-%d") 

        vFEnd= vFEnd + timedelta(days=1)
        pFEnd = vFEnd.strftime("%Y-%m-%d")

        try:  
            vSelect_sql = "SELECT * FROM api_requests where FechaHora >= ? and FechaHora <= ?"
            dbCursor = cursor.execute(vSelect_sql,(pFIni, pFEnd))
                    
            filas = dbCursor.fetchall()

            nombreFilas= [dataNombres[0] for dataNombres in cursor.description]

            datos = []

            for tfila in filas:
                fila = list(tfila)
                
                vDatos = {}
                for i in range(len(fila)):
                    vDatos[nombreFilas[i]] = fila[i]
                datos.append(vDatos)
                resu = "True"
            if resu=="True":  
                msg = "Listado obtenido satisfactoriamente"  
            else:  
                msg = "Listado obtenido vacio, no existen requests en el rango de fechas indicado"
            respuesta ={"result":resu,"msg":msg,"FIni":pFIni,"FEnd":pFEnd,"fieldnames":nombreFilas,"data":datos}
        except Exception as e:   # work on python 3.x
            respuesta ={"result":"false","msg":"Users requests not found due to ERROR -"+ str(e),"data":[]}

        vConn1.close()
        return respuesta


    def save_api_request(pDataBase, pUserId, pRequest, pIp, pDataRequest, pToken):
        resu = "False"
        vRequest = str(pRequest)
        try:
            vConn1 = sqlite3.connect(pDataBase)
            cursor2 = vConn1.cursor()

            now = datetime.now()
            vFechaHora = now.strftime("%Y-%m-%d %H:%M:%S")
#            vFechaHora = now.strftime("%Y-%m-%d")
#            insert_sql = f"INSERT INTO api_requests (userid, request, ip, data, Token, FechaHora) VALUES ({pUserId}, '{pRequest}', '{pIp}', '{pDataRequest}', '{pToken}', '{vFechaHora}')"
            insert_sql = f"INSERT INTO api_requests (userid, request, ip, data, Token, FechaHora) VALUES (?, ?, ?, ?, ?, ?)"
            cursor2.execute(insert_sql,(pUserId,vRequest,pIp,pDataRequest,pToken,vFechaHora))
            resu = "True"
            datos = {"msg":"API Request guardara Ok"}

#            print (insert_sql)
            vConn1.commit()
            vConn1.close()
        except Exception as e:   # work on python 3.x
            vConn1.close()
            datos ={"msg":"API request not saved due to ERROR -"+ str(e)}

        return {"result": resu, "data": datos}


    def save_login_request(pDataBase, pUserId, pUsername, pRol, pToken, pAccion):
        resu = "False"

        try:
            vConn1 = sqlite3.connect(pDataBase)
            cursor2 = vConn1.cursor()

            now = datetime.now()
            vFechaHora = now.strftime("%Y-%m-%d %H:%M:%S")
#            vFechaHora = now.strftime("%Y-%m-%d")
#            insert_sql = f"INSERT INTO api_requests (userid, username, rol, accion, Token, FechaHora) VALUES ({pUserId}, '{pUsername}', '{pRol}', '{pAccion}', '{pToken}', '{vFechaHora}')"
            insert_sql = f"INSERT INTO users_logins (userid, username, rol, accion, Token, FechaHora) VALUES (?, ?, ?, ?, ?, ?)"
            cursor2.execute(insert_sql,(pUserId,pUsername,pRol,pAccion,pToken,vFechaHora))
            resu = "True"
            datos = {"msg":"User Login guardado Ok"}

#            print (insert_sql)
            vConn1.commit()
        except Exception as e:   # work on python 3.x
            datos ={"msg":"User Login not saved due to ERROR -"+ str(e)}
#        finally:
#            vConn1.close()

        return {"result": resu, "data": datos}


