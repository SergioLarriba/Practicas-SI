# -*- coding: utf-8 -*-

import os
import sys, traceback, time

from sqlalchemy import create_engine, text
from pymongo import MongoClient
from sqlalchemy.orm import Session

# configurar el motor de sqlalchemy
db_engine = create_engine("postgresql://alumnodb:1234@localhost/si1", echo=False, execution_options={"autocommit":False})

# Crea la conexión con MongoDB
mongo_client = MongoClient()

def getMongoCollection(mongoDB_client):
    mongo_db = mongoDB_client.si1
    return mongo_db.topUK

def mongoDBCloseConnect(mongoDB_client):
    mongoDB_client.close();

def mongoDBStartconnect():
    return MongoClient("mongodb://localhost:27017/")

def dbConnect():
    return db_engine.connect()

def dbCloseConnect(db_conn):
    db_conn.close()

# Consultas a MongoDB
def db_Mongo_get_Movies_1994_1998():
    mongo_client=mongoDBStartconnect()
    collection = getMongoCollection(mongo_client)
    result = list( collection.find(({"genres": {"$in": ["Sci-Fi"]}, "year": {"$gte": 1994, "$lte": 1998}})) )
    mongoDBCloseConnect(mongo_client)
    return result

def db_Mongo_get_Movies_Drama_1998_The():
    mongo_client=mongoDBStartconnect()
    collection = getMongoCollection(mongo_client)
    result = list( collection.find(({"year":1998, "genres":"Drama", "title":{'$regex':'The'}})))
    mongoDBCloseConnect(mongo_client)
    return result

def db_Mongo_get_FayeDunaway_and_ViggoMortensen():
    mongo_client=mongoDBStartconnect()
    collection = getMongoCollection(mongo_client)
    result = list( collection.find(({"$and": [{"actors": "Dunaway, Faye"}, {"actors": "Mortensen, Viggo"}]})) )
    mongoDBCloseConnect(mongo_client)
    return result
  
def delState(state, bFallo, bSQL, duerme, bCommit):
    
    # Array de trazas a mostrar en la página
    dbr=[]

    # TODO: Ejecutar consultas de borrado
    # - ordenar consultas según se desee provocar un error (bFallo True) o no
    # - ejecutar commit intermedio si bCommit es True
    # - usar sentencias SQL ('BEGIN', 'COMMIT', ...) si bSQL es True
    # - suspender la ejecución 'duerme' segundos en el punto adecuado para forzar deadlock
    # - ir guardando trazas mediante dbr.append()
    
    try:
        # TODO: ejecutar consultas
        db_conn = dbConnect()
        db_session = Session(db_conn)  # Crear una nueva sesión

        # Si bSQL es True, ejecutamos las consultas sql 
        if bSQL:
            # Primero empezamos con begin
            db_session.begin()  # Iniciar una nueva transacción
            dbr.append("BEGIN")

        # Si no hay fallo, ejecutamos las consultas en orden
        if (bFallo != True):
            query_in_order(db_session, state, duerme, bCommit, dbr)
        else:
            query_not_in_order(db_session, state, duerme, bCommit, dbr)
    except Exception as e:
        if 'db_session' in locals():  # Comprueba si db_session está definida
            db_session.rollback()  # Hacer rollback de la transacción
        dbr.append("ROLLBACK" + str(e))
        if db_session is not None:
            db_session.close()  # Cerrar la sesión
    else:
        # TODO: confirmar cambios si todo va bien
        db_session.commit()  # Hacer commit de la transacción
        dbr.append("COMMIT")
        if db_session is not None:
            db_session.close()  # Cerrar la sesión

    return dbr

def query_in_order(db_conn, state, duerme, bCommit, dbr):

    # Empiezo obteniendo el id de todos los customer que estén en el estado especificado
    query = text("select customerID from customers where state='" + str(state) + "';")
    idsC = list(db_conn.execute(query))
    idCustomers = [a[0] for a in idsC]
    dbr.append("LEER CUSTOMERS")
    
    # Obtengo las orderID de las orders de los customerid de la ciudad especificados
    if idCustomers:
        query = text("select orderID from orders where customerid IN " + str(idCustomers).replace("[", "(").replace("]", ")") + ";")
        idsO = list(db_conn.execute(query))
        idOrders = [a[0] for a in idsO]
    else:
        idOrders = []
    dbr.append("LEER PEDIDOS")

    if idOrders:
        # Borro pedidos de orderdetail
        query = text("delete from orderdetail where orderid in " + str(idOrders).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR PRODUCTOS DE PEDIDOS")

    if bCommit:
        db_conn.execute(text("COMMIT;"))
        dbr.append("COMMIT")
        db_conn.execute(text("BEGIN;"))
        dbr.append("BEGIN")

    if duerme != 0:
        db_conn.execute(text("SELECT pg_sleep(" + str(duerme) + ");"))
        dbr.append("SLEEP")

    if idOrders:
        # Borro pedidos de orders
        query = text("delete from orders where orderid in " + str(idOrders).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR PEDIDOS")

    if bCommit:
        db_conn.execute(text("COMMIT;"))
        dbr.append("COMMIT")
        db_conn.execute(text("BEGIN;"))
        dbr.append("BEGIN")

    if idCustomers:
        # Borramos customers
        query = text("delete from customers where customerid in " + str(idCustomers).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR USUARIOS")

def query_not_in_order(db_conn, state, duerme, bCommit, dbr):
    # Empiezo obteniendo el id de todos los customer que estén en el state especificado
    query = text("select customerID from customers where state='" + str(state) + "';")
    idsC = list(db_conn.execute(query))
    idCustomers = [a[0] for a in idsC]
    dbr.append("LEER CUSTOMERS")
    
    # Obtengo las orderID de las orders de los customerid de la ciudad especificados
    if idCustomers:
        query = text("select orderID from orders where customerid IN " + str(idCustomers).replace("[", "(").replace("]", ")") + ";")
        idsO = list(db_conn.execute(query))
        idOrders = [a[0] for a in idsO]
    else:
        idOrders = []
    dbr.append("LEER PEDIDOS")

    if idOrders:
        # Borro pedidos de orderdetail
        query = text("delete from orderdetail where orderid in " + str(idOrders).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR PRODUCTOS DE PEDIDOS")

    if bCommit:
        db_conn.execute(text("COMMIT;"))
        dbr.append("COMMIT")
        db_conn.execute(text("BEGIN;"))
        dbr.append("BEGIN")

    if duerme != 0:
        db_conn.execute(text("SELECT pg_sleep(" + str(duerme) + ");"))
        dbr.append("SLEEP")
    
    if idCustomers:
        # Borro customers
        query = text("delete from customers where customerid in " + str(idCustomers).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR USUARIOS")
    
    if bCommit:
        db_conn.execute(text("COMMIT;"))
        dbr.append("COMMIT")
        db_conn.execute(text("BEGIN;"))
        dbr.append("BEGIN")

    if idOrders:
        # Borro pedidos de orders
        query = text("delete from orders where orderid in " + str(idOrders).replace("[", "(").replace("]", ")") + ";")
        db_conn.execute(query)
        dbr.append("BORRAR PEDIDOS")