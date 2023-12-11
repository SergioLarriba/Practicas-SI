#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from app import app
from app import database
from flask import render_template, request, url_for
import os
import sys
import time

@app.route('/', methods=['POST','GET'])
@app.route('/index', methods=['POST','GET'])
def index():
    return render_template('index.html')


@app.route('/borraEstado', methods=['POST','GET'])
def borraEstado():
    if 'state' in request.form: # Si contiene el campo state, se ha enviado el formulario -> POST
        state    = request.form["state"]
        bSQL    = request.form["txnSQL"]
        bCommit = "bCommit" in request.form
        bFallo  = "bFallo"  in request.form
        duerme  = request.form["duerme"]
        dbr = database.delState(state, bFallo, bSQL=='1', int(duerme), bCommit)
        return render_template('borraEstado.html', dbr=dbr)
    else: # Si no contiene el campo state, se ha accedido por primera vez -> GET 
        return render_template('borraEstado.html')

    
@app.route('/topUK', methods=['POST','GET'])
def topUK():
    # TODO: consultas a MongoDB ...
    movies=[[],[],[]]

    movies[0] = database.db_Mongo_get_Movies_1994_1998()
    movies[1] = database.db_Mongo_get_Movies_Drama_1998_The()
    movies[2] = database.db_Mongo_get_FayeDunaway_and_ViggoMortensen()

    return render_template('topUK.html', movies=movies)