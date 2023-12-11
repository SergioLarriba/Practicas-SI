CREATE OR REPLACE FUNCTION getTopActors(genre_name VARCHAR, OUT Actor VARCHAR, OUT Num INT, OUT Debut INT, OUT Film VARCHAR, OUT Director VARCHAR)
RETURNS SETOF RECORD AS $$
BEGIN
    RETURN QUERY
    WITH genre_actors AS (
        SELECT 
            imdb_actors.actorname AS Actor,
            CAST(COUNT(imdb_actormovies.movieid) AS INT) AS Num,
            MIN(imdb_movies.year::INT) AS Debut,
            ARRAY_AGG(imdb_movies.movietitle ORDER BY imdb_movies.year::INT) AS Films,
            ARRAY_AGG(imdb_directors.directorname ORDER BY imdb_movies.year::INT) AS Directors
        FROM 
            imdb_actors
            JOIN imdb_actormovies ON imdb_actors.actorid = imdb_actormovies.actorid
            JOIN imdb_movies ON imdb_actormovies.movieid = imdb_movies.movieid
            JOIN imdb_moviegenres ON imdb_movies.movieid = imdb_moviegenres.movieid
            JOIN genres ON imdb_moviegenres.genre = genres.genreName
            JOIN imdb_directormovies ON imdb_movies.movieid = imdb_directormovies.movieid
            JOIN imdb_directors ON imdb_directormovies.directorid = imdb_directors.directorid
        WHERE
            genres.genrename = genre_name
        GROUP BY 
            imdb_actors.actorname
        HAVING 
            CAST(COUNT(imdb_actormovies.movieid) AS INT) > 4
    )
    SELECT 
        genre_actors.Actor, 
        genre_actors.Num,
        genre_actors.Debut,
        genre_actors.Films[1] AS Film,
        genre_actors.Directors[1] AS Director
    FROM 
        genre_actors
    ORDER BY 
        genre_actors.Num DESC;
END;
$$ LANGUAGE plpgsql;