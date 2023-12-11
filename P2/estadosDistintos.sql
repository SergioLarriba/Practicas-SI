--Creamos los indices para mejorar el rendimiento de la consulta
CREATE INDEX IF NOT EXISTS idx_customers_country_customerid ON customers (country, customerid);
CREATE INDEX IF NOT EXISTS idx_orders_orderyear_customerid ON orders (EXTRACT(YEAR FROM orderdate), customerid);

--DROP INDEX IF EXISTS idx_customers_country_customerid;
--DROP INDEX IF EXISTS idx_orders_orderyear_customerid;


--consulta con un EXPLAIN para ver el plan de ejecución.
EXPLAIN
SELECT COUNT(DISTINCT c.state)
FROM public.customers c
JOIN public.orders o ON c.customerid = o.customerid
WHERE EXTRACT(YEAR FROM o.orderdate) = 2017
AND c.country = 'Peru';

-- Consulta para obtener el resultado.
SELECT COUNT(DISTINCT c.state)
FROM public.customers c
JOIN public.orders o ON c.customerid = o.customerid
WHERE EXTRACT(YEAR FROM o.orderdate) = 2017
AND c.country = 'Peru';