CREATE OR REPLACE PROCEDURE setOrderAmount()
LANGUAGE plpgsql
AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    -- Actualizo netamount
    WITH precios AS (
        SELECT od.orderid, SUM(od.price * od.quantity) AS price
        FROM orderdetail od
        JOIN orders o ON o.orderid = od.orderid
        WHERE o.netamount IS NULL
        GROUP BY od.orderid
    )
    UPDATE orders
    SET netamount = precios.price
    FROM precios
    WHERE orders.netamount IS NULL and orders.orderid = precios.orderid;

 

    --Actualizo totalamount
    UPDATE orders
    SET totalamount = netamount * ((tax / 100) + 1)
    WHERE totalamount IS NULL AND netamount IS NOT NULL AND tax IS NOT NULL;


END;
$$;

CALL setOrderAmount();