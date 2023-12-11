#!/bin/env python3

import json
import os
import shutil
from quart import Quart, jsonify, request
from quart.helpers import make_response

# Defaults
USERDIR = "./build/users"

app = Quart(__name__)


@app.route('/', methods=["GET"])
async def hello():
    return 'hello'


@app.delete('/user/<username>')
async def user_delete(username):
    resp = {}
    user_directory = os.path.join(USERDIR, username)

    if os.path.exists(user_directory):
        try:
            shutil.rmtree(user_directory)
            resp["status"] = "OK"
        except Exception as e:
            resp["status"] = "KO"
            resp["error"] = f"Error al eliminar el usuario: {str(e)}"
    else:
        resp["status"] = "KO"
        resp["error"] = "El usuario no existe"

    return jsonify(resp), 200 if resp["status"] == "OK" else 400


@app.patch('/user/<username>')
async def user_patch(username):
    resp = {}
    user_directory = os.path.join(USERDIR, username)

    if os.path.exists(user_directory):
        try:
            with open(f"{user_directory}/data.json", "r") as ff:
                existing_data = json.load(ff)

            data = await request.get_json()
            existing_data.update(data)

            with open(f"{USERDIR}/{username}/data.json", "w") as ff:
                json.dump(existing_data, ff)

            resp["status"] = "OK"
        except Exception as e:
            resp["status"] = "KO"
            resp["error"] = f"Error abriendo user dir: {str(e)}"
    else:
        resp["status"] = "KO"
        resp["error"] = "El usuario no existe"

    return jsonify(resp), 200 if resp["status"] == "OK" else 400


@app.get('/user/<username>')
async def user_get(username):
    resp = {}
    with open(f"{USERDIR}/{username}/data.json", "r") as ff:
        resp = json.loads(ff.read())
    return await make_response(jsonify(resp), 200)


@app.put('/user/<username>')
async def user_put(username):
    resp = {}
    try:
        os.mkdir(f"{USERDIR}/{username}")
    except Exception as e:
        resp["status"] = "KO"
        resp["error"] = f"Error creando user dir: {str(e)}"
        return await make_response(jsonify(resp), 400)

    data = await request.get_json()
    data["username"] = username
    with open(f"{USERDIR}/{username}/data.json", "w") as ff:
        ff.write(json.dumps(data))

    resp["status"] = "OK"
    return await make_response(jsonify(resp), 200)


if __name__ == "__main__":
    app.run(host='localhost', port=5000)
