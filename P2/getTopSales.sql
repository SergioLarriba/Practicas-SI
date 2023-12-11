
CREATE OR REPLACE FUNCTION getTopSales(year1 INT, year2 INT, OUT Year INT, OUT Film CHAR, OUT sales BIGINT)
RETURNS SETOF RECORD
AS $$
BEGIN
    RETURN QUERY 
    SELECT yearly_sales.yearofsale::INT, CAST(m.movietitle AS CHAR(100)), yearly_sales.totalsales
    FROM (
        SELECT
            extract(YEAR FROM o.orderdate)::INT AS yearofsale,
            p.movieid,
            sum(od.quantity) AS totalsales
        FROM
            orderdetail od
            JOIN orders o ON od.orderid = o.orderid
            JOIN products p ON od.prod_id = p.prod_id
        WHERE
            extract(YEAR FROM o.orderdate) BETWEEN year1 AND year2
        GROUP BY
            yearofsale, p.movieid
    ) AS yearly_sales
    JOIN (
        SELECT
            yearofsale,
            max(totalsales) AS maxsales
        FROM (
            SELECT
                extract(YEAR FROM o.orderdate)::INT AS yearofsale,
                p.movieid,
                sum(od.quantity) AS totalsales
            FROM
                orderdetail od
                JOIN orders o ON od.orderid = o.orderid
                JOIN products p ON od.prod_id = p.prod_id
            WHERE
                extract(YEAR FROM o.orderdate) BETWEEN year1 AND year2
            GROUP BY
                yearofsale, p.movieid
        ) AS inner_query
        GROUP BY
            yearofsale
    ) AS max_sales_per_year ON yearly_sales.yearofsale = max_sales_per_year.yearofsale AND yearly_sales.totalsales = max_sales_per_year.maxsales
    JOIN imdb_movies m ON yearly_sales.movieid = m.movieid
    ORDER BY
        yearly_sales.yearofsale DESC, yearly_sales.totalsales DESC;
END;
$$
LANGUAGE plpgsql;
