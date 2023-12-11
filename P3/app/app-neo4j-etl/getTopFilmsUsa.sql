DROP FUNCTION IF EXISTS getTopFilmsUsa();

CREATE OR REPLACE FUNCTION getTopFilmsUsa(OUT movietitle VARCHAR, OUT ventas INT)
RETURNS SETOF RECORD
AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        CAST(m.movietitle AS VARCHAR(100)), 
        SUM(quantity)::INT
    FROM orders o 
    JOIN orderdetail od ON o.orderid = od.orderid
    JOIN products p ON p.prod_id = od.prod_id
    JOIN imdb_movies m ON m.movieid = p.movieid
    JOIN imdb_moviecountries ic ON ic.movieid = m.movieid
    WHERE ic.country = 'USA'
    GROUP BY m.movietitle
    ORDER BY SUM(quantity) DESC
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;