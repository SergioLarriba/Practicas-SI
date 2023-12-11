
import psycopg2
from neo4j import GraphDatabase

# Conexion a la base de datos si1  
print("Conectando a la base de datos si1...")
postgres_conn = psycopg2.connect(
    host="localhost",
    database="si1",
    user="alumnodb",
    password="1234"
)

# Conexion a la base de datos neo4j
print("Conectando a la base de datos neo4j...")
neo4j_driver = GraphDatabase.driver("bolt://44.204.51.133:7687", auth=("neo4j", "jewel-filler-frosts"))
neo4j_session = neo4j_driver.session()

# Obtener las 20 peliculas mas vendidas de USA
print("Obteniendo las 20 peliculas mas vendidas de USA...")
postgres_cursor = postgres_conn.cursor()
postgres_cursor.execute("""
    WITH top_movies AS (
        SELECT m.movieid, m.movietitle
        FROM orders o 
        JOIN orderdetail od ON o.orderid = od.orderid
        JOIN products p ON p.prod_id = od.prod_id
        JOIN imdb_movies m ON m.movieid = p.movieid
        JOIN imdb_moviecountries ic ON ic.movieid = m.movieid
        WHERE ic.country = 'USA'
        GROUP BY m.movieid, m.movietitle
        ORDER BY SUM(od.quantity) DESC
        LIMIT 20
    )
    SELECT tm.movieid, tm.movietitle, a.actorid, a.actorname, d.directorid, d.directorname
    FROM top_movies tm
    LEFT JOIN imdb_actormovies am ON am.movieid = tm.movieid
    LEFT JOIN imdb_actors a ON a.actorid = am.actorid
    LEFT JOIN imdb_directormovies dm ON dm.movieid = tm.movieid
    LEFT JOIN imdb_directors d ON d.directorid = dm.directorid;
""")
movies = postgres_cursor.fetchall()

# Crear nodos y relaciones en neo4j
print("Creando nodos y relaciones en neo4j...")
with neo4j_driver.session(database="neo4j") as session:
    for movie in movies:
        if movie[0] is not None and movie[1] is not None:
            session.execute_write(
                lambda tx: tx.run("""
                    MERGE (m:Movie {id: $movieid, title: $movietitle})
                    """, movieid=movie[0], movietitle=movie[1])
            )
        if movie[2] is not None and movie[3] is not None:
            session.execute_write(
                lambda tx: tx.run("""
                    MERGE (a:Actor:Person {id: $actorid, name: $actorname})
                    MERGE (a)-[:ACTED_IN]->(m)
                    """, actorid=movie[2], actorname=movie[3], movieid=movie[0])
            )
        if movie[4] is not None and movie[5] is not None:
            session.execute_write(
                lambda tx: tx.run("""
                    MERGE (d:Director:Person {id: $directorid, name: $directorname})
                    MERGE (d)-[:DIRECTED]->(m)
                    """, directorid=movie[4], directorname=movie[5], movieid=movie[0])
            )

print("Terminado!")
neo4j_driver.close()
