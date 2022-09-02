# for persists data
import sqlite3

# for proccessing data
import pandas as pd

from datetime import datetime
from datetime import timedelta

import hashlib



#--- clase para acceso a datos SqLite ----
class UserControl:

    def genera_token():
        now = datetime.now()
        plainToken = now.strftime("%m/%d/%Y, %H:%M:%S,f")

        h = hashlib.new("sha1", plainToken.encode('utf-8'))
        return str(h.hexdigest())
     

    def check_token(pDataBase, pToken):
        vConn1 = sqlite3.connect(pDataBase)    
        username = "-"
        user_id = -1
        vRol = "-"

        try:
            cursor = vConn1.cursor()
            select_sql = "SELECT *,? FROM users WHERE Token= ?"
            dbCursor = cursor.execute(select_sql, ("1", pToken))
            fila = dbCursor.fetchone()

            if fila == None:
                resultqry="Token no encontrado"
                resu = "False"
                pToken = "Token no encontrado"
            else:
                resultqry="Ok"
                resu = "True"
                username = fila[1]
                user_id = str(fila[0])
                vRol = fila[7]

        except:
            resultqry="Error en "+select_sql
            pToken = "[]"
            resu = "False"

        vConn1.close()

        datos ={"username":username,"user_id":user_id,"rol":vRol,"resultqry":resultqry, "Token":pToken}
        return {"result": resu, "data": datos}


    def user_table_created(pDataBase):
        vConn1 = sqlite3.connect(pDataBase)    
        vDfP = {}
        usertable_found = False

        try:  
            vSelect_sql = "SELECT name FROM sqlite_master WHERE type='table' and name='users'"
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
            for i in range(len(vDfP.name)):
                if vDfP.name[i] == 'users':
                    usertable_found = True
        except:
            usertable_found = False

        vConn1.close()
        return usertable_found



    def get_user(pDataBase, username):
        vConn1 = sqlite3.connect(pDataBase)    

        vDfP = {}
        try:  
            vSelect_sql = "SELECT * FROM users WHERE username='"+username+"'";
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
        except:
            vDfP = {}

        vConn1.close()
        return vDfP



    def get_user_list(pDataBase, pUserId):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        respuesta ={}
        resu = "False"

        try:  
            if pUserId<1:
                vSelect_sql = "SELECT * FROM users "
                dbCursor = cursor.execute(vSelect_sql)
            else:
                vSelect_sql = "SELECT * FROM users where id = ? and ?=?"
                dbCursor = cursor.execute(vSelect_sql,(pUserId,1,1))

            filas = dbCursor.fetchall()

            nombreFilas= [dataNombres[0] for dataNombres in cursor.description]
            nombreFilas.pop(2)

            datos = []

            for tfila in filas:
                fila = list(tfila)
                fila.pop(2)
                
                vDatos = {}
                for i in range(len(fila)):
                    vDatos[nombreFilas[i]] = fila[i]
                datos.append(vDatos)
                resu = "True"
            if resu=="True":  
                msg = "Listado obtenido satisfactoriamente"  
            else:  
                msg = "Listado obtenido vacio, no existe el id " + str(pUserId)
            respuesta ={"result":resu,"msg":msg,"fieldnames":nombreFilas,"data":datos}
        except Exception as e:   # work on python 3.x
            respuesta ={"result":"false","msg":"User list not found due to ERROR -"+ str(e),"data":[]}

        vConn1.close()
        return respuesta



    def get_user_by_id(pDataBase, user_id):
        vConn1 = sqlite3.connect(pDataBase)    
        cursor = vConn1.cursor()
        datos ={}
        resu = "False"

        try:  
            vSelect_sql = "SELECT *, ? FROM users WHERE id= ?"
            dbCursor = cursor.execute(vSelect_sql, ("1",user_id))

            fila = dbCursor.fetchone()
            if fila == None:
                datos = {"username":"-","msg":"Username not found for user_id: "+str(user_id)}
            else:
                datos ={"username":fila[1],"user_id":fila[0]}
                resu = "True"
        except Exception as e:   # work on python 3.x
            datos ={"username":"-","msg":"Username not found due to ERROR -"+ str(e)}

        vConn1.close()
        return {"result": resu, "data": datos}


    def user_signup(pDataBase, pUserName, pUserPwd):
        vConnS = sqlite3.connect(pDataBase)
        cursor = vConnS.cursor()

        table_found = UserControl.user_table_created(pDataBase)
        if table_found == False:  # crear metodo para crear tablas
            UserControl.create_user_data_struct(pDataBase)

        select_sql = "SELECT username FROM users WHERE username='"+pUserName+"'"
        dbCursor = cursor.execute(select_sql)

        fila = dbCursor.fetchone()
        if fila == None:

            now = datetime.now()
            plainKey = now.strftime("%m/%d/%Y, %H:%M:%S,f")
            plainSecret = plainKey+pUserName

            encodedKey = hashlib.md5(plainKey.encode('utf-8')).hexdigest()
            encodedSecret = hashlib.md5(plainSecret.encode('utf-8')).hexdigest()

            insert_sql = f"INSERT INTO users (username, password, ClientKey, ClientSecret) VALUES ('{pUserName}', '{pUserPwd}', '{encodedKey}', '{encodedSecret}')"
            cursor.execute(insert_sql)
            resu = "True"

            select_sql = "SELECT id, username, ClientKey, ClientSecret, rol FROM users WHERE username='"+pUserName+"'"
            dbCursor = cursor.execute(select_sql)
            vFila = dbCursor.fetchone()
            datos = {"msg":"UserFound", "userid":vFila[0], "username":vFila[1], "ClientKey":vFila[2], "ClientSecret":vFila[3], "Token":"No", "userrol":vFila[4]}

        else:
            resu = "False"
            datos ={"msg":"Nombre de usuario ya registrado"}
        vConnS.commit()
#        vConnS.close()

        return {"result": resu, "data": datos}


    def create_user_data_struct(pDataBase):
        datos = {"msg":"DatabaseError NoExec"}

        try:
            vConn = sqlite3.connect(pDataBase)
            cursor = vConn.cursor()

            resu = "False"

            user_found = UserControl.user_table_created(pDataBase)

            if user_found == False:  # crear metodo para crear tablas
                create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME default null, rol VARCHAR(5) NOT NULL DEFAULT 'USR')"
                cursor.execute(create_sql)
                index_sql = "CREATE INDEX idxUsers ON users (id)"
                cursor.execute(index_sql)

                # Crear tabla para registrar los logins: id, user_id, username, Token, FechaHora
                create_sql = "CREATE TABLE IF NOT EXISTS users_logins (id INTEGER PRIMARY KEY AUTOINCREMENT, userid INTEGER, username VARCHAR (30), Token VARCHAR (255), FechaHora DATETIME default null, rol VARCHAR(5) NOT NULL DEFAULT 'USR', accion VARCHAR(10) NOT NULL DEFAULT 'LOGIN')"
                cursor.execute(create_sql)
                index_sql = "CREATE INDEX idxUserLogins ON users_logins (id)"
                cursor.execute(index_sql)

                # Crear tabla para registrar los API request userid, request, ip, data, Token, FechaHora
                create_sql = "CREATE TABLE IF NOT EXISTS api_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, userid INTEGER, request VARCHAR (255), ip VARCHAR (127), data VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME)"
                cursor.execute(create_sql)
                index_sql = "CREATE INDEX idxApiRequests ON api_requests (id)"
                cursor.execute(index_sql)

                vConn.commit()
                datos = {"msg":"Tablas creadas"}
                resu = "True"
            else:
                datos = {"msg":"Tablas ya existen"}

            vConn.close()
        except:
            datos = {"msg":"DatabaseError Except"}

        return {"result": resu, "data": datos}



    def user_login(pDataBase, pUserName, pUserPwd, pClientKey):
        vConnL = sqlite3.connect(pDataBase)
        cursor = vConnL.cursor()

        table_found = UserControl.user_table_created(pDataBase)
        if table_found == False:  # crear metodo para crear tablas
            UserControl.create_user_data_struct(pDataBase)

        select_sql = "SELECT *, ? FROM users WHERE username= ?"
        dbCursor = cursor.execute(select_sql, ("1",pUserName))

        datos = {}
        fila = dbCursor.fetchone()
        if fila != None:
            if fila[2] == pUserPwd and fila[3] == pClientKey:
                vToken= UserControl.genera_token()
                update_sql = "UPDATE users SET Token= ? WHERE id= ?"
                try:
                    cursor2 = vConnL.cursor()
                    resultqry = cursor2.execute(update_sql, (vToken, fila[0]))
#                    rows_affected = cursor2.rowcount
                    resu = "True"
                except:
                    resultqry="Error en "+update_sql
                    vToken = "Error guardando token generado"
                    resu = "False"
                    
                datos ={"msg":"UserFound", "resultqry":resultqry, "userid":fila[0], "username":fila[1], "Token":vToken, "userrol":fila[7]}
            else:
                resu = "False"
                datos ={"msg":"Usuario, clave o keys incorrectos."}
        else:
            resu = "False"
            datos ={"msg":"Usuario no registrado."}

        vConnL.commit()
#        vConnL.close()

        return {"result": resu, "data": datos}


    def user_change_rol(pDataBase, pUserIdClient, pNuevoRol):
        vConnL = sqlite3.connect(pDataBase)
        cursor = vConnL.cursor()

        select_sql = "SELECT id, username, rol, ? FROM users WHERE id= ?"
        dbCursor = cursor.execute(select_sql, ("1",pUserIdClient))

#-- Puede almacenarse cambio de rol si se desea, creando la correspondiente tabla

        datos = {}
        fila = dbCursor.fetchone()
        resu = "False"

        if fila != None:
            vViejoRol = fila[2]
            if vViejoRol != pNuevoRol:
                update_sql = "UPDATE users SET rol= ? WHERE id= ?"
                try:
                    cursor2 = vConnL.cursor()
                    resultqry = cursor2.execute(update_sql, (pNuevoRol, pUserIdClient))
                    if cursor2.rowcount>0:
                        resu = "True"
                except:
                    resultqry="Error en "+update_sql
            else:
                resultqry="El nuevo rol es igual al anterior"

            datos ={"result":resu, "msg":"UserFound", "userid":fila[0], "username":fila[1], "viejorol":vViejoRol, "nuevorol":pNuevoRol, "resultqry":resultqry}
        else:
            datos ={"result":resu, "msg":"Usuario no registrado."}

        vConnL.commit()
#        vConnL.close()

        return {"result": resu, "data": datos}




                    



