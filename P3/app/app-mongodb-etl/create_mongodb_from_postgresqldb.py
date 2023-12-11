import os
import sys
import traceback
import time
import pymongo
from sqlalchemy import create_engine, text
from pymongo import MongoClient

# Configurar el motor de sqlalchemy
"""
Url con: 
    Usuario: alumnodb
    Contraseña: 1234
    Base de datos: si1
    host: localhost
"""
db_engine = create_engine("postgresql://alumnodb:1234@localhost/si1", echo=False)

# Configurar mongodb -> mongodb_client es el cliente que usaremos para interactuar con mongodb
mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")

# Creamos una bas de datos en mongodb llamada si1 y una coleccion llamada france
mongodb = mongodb_client["si1"] # Base de datos que estoy utilizando
collection_france = mongodb["france"] 

try:
    # Conexion a la base de datos de postgress
    db_connection = None
    db_connection = db_engine.connect()

    # Creamos una tabla con las peliculas francesas de la base de datos 
    print("Creando tabla de peliculas francesas...")
    france_table = text("CREATE TEMPORARY TABLE actualFRMovies AS (\
                        SELECT m.movieid, m.movietitle, m.year\
                        FROM imdb_moviecountries mc\
                        JOIN imdb_movies m ON mc.movieid = m.movieid\
                        WHERE mc.country = 'France'\
                        ORDER BY m.year DESC\
                        )"
                        )

    # Ejecuta la consulta CREATE TABLE
    db_connection.execute(france_table)

    # Lista con las peliculas francesas de la base de datos 
    movieList = list(db_connection.execute(text("SELECT * FROM actualFRMovies")))
    recentFRIds = list(db_connection.execute(text("SELECT movieid FROM actualFRMovies")))

    moviesDocument = []
    print("Obteniendo los datos de las peliculas en Postgres y pasandolos a mongoDB...")
    # Conseguimos todos los datos de las peliculas francesas para pasarlos a mongodb
    for movie in movieList: 
        # Obtenemos el id, titulo, genero, año, directores y actores de las peliculas 
        movieId = movie[0]
        movieTitle = movie[1]
        movieYear = int(movie[2])

        # Obtenemos los generos de la pelicula
        db_movieGenres = list(db_connection.execute(text("select genre from imdb_moviegenres\
                                               where movieid = '" + str(movieId) + "'")))
        movieGenres = []
        for genre in db_movieGenres:
            movieGenres.append(genre[0])
        
        # Obtenemos los directores de la pelicula
        db_movieDirectors = list(db_connection.execute(text("select directorname from imdb_directormovies, imdb_directors\
                                                  where movieid = '" + str(movieId) + "' and\
                                                  imdb_directormovies.directorid = imdb_directors.directorid")))
        movieDirectors = []
        for directorname in db_movieDirectors:
            movieDirectors.append(directorname[0])

        # Obtenemos los actores de la pelicula
        db_movieActors = list(db_connection.execute(text("select actorname from imdb_actormovies, imdb_actors\
                                               where movieid = '" + str(movieId) + "' and\
                                               imdb_actormovies.actorid = imdb_actors.actorid")))
        movieActors = []
        for actorname in db_movieActors:
            movieActors.append(actorname[0])
        
        # Creamos y añadimos a la coleccion el documento para la pelicula en mongoDB
        moviesDocument.append({
            'title': movieTitle,
            'genres': movieGenres,
            'year': movieYear,
            'directors': movieDirectors,
            'actors': movieActors,
        })

    # Insertamos las peliculas a mongodb
    collection_france.insert_many(moviesDocument)

    # Obtenemos las peliclulas mas relacionadas y relacionadas
    max_related_movies = 10
    movies = list(collection_france.find())
    for movie in movies:
        # Coincidencia 100% -> Busco todas las peliculas que tengan los mismos generos que la pelicula actual 
        most_related = collection_france.find(
            {
                'genres': {'$all': movie['genres']},
                '_id': {'$ne': movie['_id']}
            },
            {
                'title': 1,
                'year': 1,
                '_id': 0
            }
        ).sort('year', pymongo.DESCENDING).limit(max_related_movies)
        most_related = list(most_related)

        # Coincidencia 50% 
        # Contamos numeros de coincidencia con un 'group_by'
        aggregate = collection_france.aggregate([
            {'$unwind': '$genres'}, # Descompongo los documentos por generos 
            {'$match': {'genres': {'$in': movie['genres']}}}, # Filtro los documentos que tengan al menos un genero en comun con la pelicula actual 
            {'$group': { # Agrupo los documentos por id, titulo y año y cuento el numero de coincidencias
                '_id': {'_id': '$_id', 'title': '$title', 'year': '$year'},
                'number': {'$sum': 1}
            }},
            {'$match': { # Filtro los documentos que tengan un numero de coincidencias entre 50% y 100%
                'number': {
                    '$gte': len(movie['genres'])*0.5,
                    '$lt': len(movie['genres'])
                }
            }},
            {'$sort': { # Ordeno los documentos por numero de coincidencias
                'number': pymongo.DESCENDING
            }},
            {'$limit': max_related_movies}, 
            {'$sort': {
                '_id.year': pymongo.DESCENDING
            }}
        ])
        related = [{'title': rel['_id']['title'], 'year': rel['_id']['year']}
                   for rel in aggregate]
        
        # Insertamos aquellos con 100% > number > 50%
        collection_france.update_one({'_id': movie['_id']}, {'$set': {
            'most_related_movies': most_related,
            'related_movies': related
        }})
        
    print("Películas insertadas a mongodb correctamente")

    # Desconectamos la base de datos de postgres
    if db_connection is not None:
        db_connection.close()
    
except: 
    # Control de errores en la base de datos
    if db_connection is not None:
        db_connection.close()
    print("Exception in DB access:")
    print("-"*60)
    traceback.print_exc(file=sys.stderr)
    print("-"*60)







    
    







