CREATE OR REPLACE FUNCTION updInventoryAndCustomer_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'Paid' THEN

        -- Actualizamos orders con la fecha y hora actual
        UPDATE orders SET orderdate = CURRENT_DATE
        WHERE NEW.orderid = orderid;

        -- Actualizamos el inventario con una subconsulta
        UPDATE inventory
        SET stock = stock - od.quantity,
            sales = sales + od.quantity
        FROM orderdetail AS od
        WHERE od.orderid = NEW.orderid
        AND inventory.prod_id = od.prod_id;

        -- Insertamos alertas para productos con stock = cero
        INSERT INTO alerts (prod_id, alert_date, alert_time)
        SELECT inventory.prod_id, CURRENT_DATE, CURRENT_TIME
        FROM inventory
        WHERE inventory.stock = 0
        AND inventory.prod_id IN (SELECT prod_id FROM orderdetail WHERE orderid = NEW.orderid);

        -- Pagar puntos de fidelidad y descontar precio de balance
        UPDATE customers
        SET loyalty = loyalty + floor(0.05 * NEW.totalamount),
            balance = balance - NEW.totalamount
        WHERE customers.cust_id = (SELECT cust_id FROM orders WHERE orderid = NEW.orderid);

    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER updInventoryAndCustomer
AFTER UPDATE OF status ON orders
FOR EACH ROW
EXECUTE PROCEDURE updInventoryAndCustomer_trigger();
