# for persists data
import sqlite3

# for proccessing data
import pandas as pd

from datetime import datetime

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

        except:
            resultqry="Error en "+select_sql
            pToken = "[]"
            resu = "False"

        vConn1.close()

        datos ={"username":username,"user_id":user_id,"resultqry":resultqry, "Token":pToken}
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


    def user_signin(pDataBase, pUserName, pUserPwd):
        vConn = sqlite3.connect(pDataBase)
        cursor = vConn.cursor()

        user_found =  UserControl.user_table_created(pDataBase)

        if user_found == False:
            create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME)"
            cursor.execute(create_sql)
            index_sql = "CREATE INDEX idxUsers ON users (id)"
            cursor.execute(index_sql)

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

            select_sql = "SELECT username, ClientKey, ClientSecret FROM users WHERE username='"+pUserName+"'"
            dbCursor = cursor.execute(select_sql)
            datos = dbCursor.fetchone()
        else:
            resu = "False"
            datos ={"msg":"Nombre de usuario ya registrado"}
        vConn.commit()
        vConn.close()

        return {"result": resu, "data": datos}


    def create_user_data_struct(pDataBase):
        datos = {"msg":"DatabaseError NoExec"}

        try:
            vConn = sqlite3.connect(pDataBase)
            cursor = vConn.cursor()

            resu = "False"

            user_found = UserControl.user_table_created(pDataBase)

            if user_found == False:  # crear metodo para crear tablas
                create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME)"
                cursor.execute(create_sql)
                index_sql = "CREATE INDEX idxUsers ON users (id)"
                cursor.execute(index_sql)

                # Crear tabla para registrar los logins: id, user_id, username, Token, FechaHora
                create_sql = "CREATE TABLE IF NOT EXISTS users_logins (id INTEGER PRIMARY KEY AUTOINCREMENT, userid INTEGER, username VARCHAR (30), Token VARCHAR (255), FechaHora DATETIME)"
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



    def user_login(pDataBase, pUserName, pUserPwd, pClientKey, pClientSecret):
        vConn = sqlite3.connect(pDataBase)
        cursor = vConn.cursor()

        user_found = UserControl.user_table_created(pDataBase)

        if user_found == False:  # crear metodo para crear tablas
            create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME)"
            cursor.execute(create_sql)
            index_sql = "CREATE INDEX idxUsers ON users (id)"
            cursor.execute(index_sql)

            # Crear tabla para registrar los logins: id, user_id, username, Token, FechaHora
            create_sql = "CREATE TABLE IF NOT EXISTS users_logins (id INTEGER PRIMARY KEY AUTOINCREMENT, userid INTEGER, username VARCHAR (30), Token VARCHAR (255), FechaHora DATETIME)"
            cursor.execute(create_sql)
            index_sql = "CREATE INDEX idxUserLogins ON users_logins (id)"
            cursor.execute(index_sql)

            # Crear tabla para registrar los API request userid, request, ip, data, Token, FechaHora
            create_sql = "CREATE TABLE IF NOT EXISTS api_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, userid INTEGER, request VARCHAR (255), ip VARCHAR (127), data VARCHAR (255), Token VARCHAR (255), FechaHora DATETIME)"
            cursor.execute(create_sql)
            index_sql = "CREATE INDEX idxUserLogins ON users_logins (id)"
            cursor.execute(index_sql)

        select_sql = "SELECT *, ? FROM users WHERE username= ?"
        dbCursor = cursor.execute(select_sql, ("1",pUserName))

        datos = {}
        fila = dbCursor.fetchone()
        if fila != None:
            if fila[2] == pUserPwd and fila[3] == pClientKey and fila[4] == pClientSecret :
                vToken= UserControl.genera_token()
                try:
                    cursor2 = vConn.cursor()
                    update_sql = "UPDATE users SET Token= ? WHERE id= ?"
                    resultqry = cursor2.execute(update_sql, (vToken, fila[0]))
#                    rows_affected = cursor2.rowcount
                    resu = "True"

                except:
                    resultqry="Error en "+update_sql
                    vToken = "Error guardando token generado"
                    resu = "False"

                datos ={"username":fila[1],"resultqry":resultqry, "Token":vToken}
            else:
                resu = "False"
                datos ={"msg":"Usuario, clave o keys incorrectos."}
        else:
            resu = "False"
            datos ={"msg":"Usuario no registrado."}

        vConn.commit()
        vConn.close()

        return {"result": resu, "data": datos}


