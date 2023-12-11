from pymongo import MongoClient
from sqlalchemy import create_engine, text 

MONGO_URI = "mongodb://localhost:27017/"
MONGO_ENGINE = "postgresql://alumnodb:1234@localhost/si1"

# Conectarse a la base de datos si1 y devolvemos el cliente para 
def connect_to_mongodb():
    # db_engine para interactuar con la base de datos por SQLAlchemy
    db_engine = create_engine(MONGO_ENGINE, echo=False)
    # mongodb_client para interactuar con la base de datos por pymongo
    mongodb_client = MongoClient(MONGO_URI)
    # Base de datos 
    db = mongodb_client["si1"]
    # Coleccion "France"
    collection_france = db["france"] 
    return collection_france, db_engine 


def main():
    collection_france, db_engine = connect_to_mongodb()
    # Establecemos conexion con la base de datos 
    db_connection = None
    db_connection = db_engine.connect()

    # Query para dar una tabla con toda la información de aquellas películas (documentos) de ciencia ficción
    # comprendidas entre 1994 y 1998.
    print("Ejecutando query 1...")
    query = {"genres": {"$in": ["Sci-Fi"]}, "year": {"$gte": 1994, "$lte": 1998}}
    films_between_1994_1998 = list(collection_france.find(query))

    # Imprimo los resultados 
    print("Resultados de la query 1:")
    for film in films_between_1994_1998:
        print(film)

    # Query para dar una tabla con toda la información de aquellas películas (documentos) que sean dramas del
    # año 1998, y que empiecen por la palabra “The” (por ejemplo "Governess, The").
    print("Ejecutando query 2...")
    query = {"year":1998, "genres":"Drama", "title":{'$regex':'The'}}
    films_drama_1998 = list(collection_france.find(query))

    # Imprimo los resultados
    print("Resultados de la query 2:")
    for film in films_drama_1998:
        print(film)

    # Query para dar una tabla con toda la información de aquellas películas (documentos) en las que Faye
    # Dunaway y Viggo Mortensen hayan compartido reparto.
    print("Ejecutando query 3...")
    query = {"$and": [{"actors": "Dunaway, Faye"}, {"actors": "Mortensen, Viggo"}]}
    films_faye_viggo = list(collection_france.find(query)) 

    # Imprimo los resultados
    print("Resultados de la query 3:")
    for film in films_faye_viggo:
        print(film)
    
    # Cerramos la conexion con la base de datos
    db_connection.close()

if __name__ == "__main__":
    main() 

    
    