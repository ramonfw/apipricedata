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
        username="-"

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
                username=fila[1]

        except:
            resultqry="Error en "+select_sql
            pToken = "[]"
            resu = "False"

        datos ={"username":username,"resultqry":resultqry, "Token":pToken}
        return {"result": resu, "data": datos}


    def get_users_list(pDataBase):
        vConn1 = sqlite3.connect(pDataBase)    
        vDfP = {}

        try:  
            vSelect_sql = "SELECT username FROM users limit 2"
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
            vConn1.close()
        except:
            vDfP = {}

        return vDfP


    def get_user(pDataBase, username):
        vConn1 = sqlite3.connect(pDataBase)    

        vDfP = {}
        try:  
            vSelect_sql = "SELECT * FROM users WHERE username='"+username+"'";
            vDfP = pd.read_sql_query(vSelect_sql, vConn1)
            vConn1.close()
        except:
            vDfP = {}

        return vDfP


    def user_signin(pDataBase, pUserName, pUserPwd):
        vConn = sqlite3.connect(pDataBase)
        cursor = vConn.cursor()

        list_users = UserControl.get_users_list(pDataBase)
        user_found = False

        if len(list_users)>0:
            user_found = True

        create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255))"
        cursor.execute(create_sql)

        if user_found == False:
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
            fila = dbCursor.fetchone()
        else:
            resu = "False"

        vConn.commit()
        vConn.close()

        return {"result": resu, "data": fila}


    def user_login(pDataBase, pUserName, pUserPwd, pClientKey, pClientSecret):
        vConn = sqlite3.connect(pDataBase)
        cursor = vConn.cursor()

        list_users = UserControl.get_users_list(pDataBase)
        user_found = False

        if len(list_users.username)>0:
            user_found = True

        create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR (30), password VARCHAR (30), ClientKey VARCHAR (255), ClientSecret VARCHAR (255), Token VARCHAR (255))"
        cursor.execute(create_sql)

        if user_found == False:
            index_sql = "CREATE INDEX idxUsers ON users (id)"
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
        else:
            resu = "False"

        vConn.commit()

        return {"result": resu, "data": datos}


