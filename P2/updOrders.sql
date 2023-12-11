-- Primero, creamos la función que se llamará cada vez que el trigger se dispare
CREATE OR REPLACE FUNCTION updOrders_trigger()
RETURNS TRIGGER AS $$
DECLARE
    oid INTEGER;
    neto NUMERIC;
    local_tax NUMERIC;
BEGIN

    --Actualizo el orderid dependiendo de la operacion realizada
    oid := COALESCE(NEW.orderid, OLD.orderid);

    --Obtengo impuesto
    SELECT tax INTO local_tax FROM orders WHERE orderid=oid;

    --Calculo neto
    SELECT sum(price*quantity) INTO neto
    FROM orderdetail
    WHERE orderid = oid;

    --Si no hay pedido se pone a 0
    IF neto IS NULL THEN
        neto := 0;
    END IF;

    --ACTUALIZO ORDERS
    UPDATE orders
    set netamount = round(neto,2),
        totalamount = round(neto*(1+local_tax/100),2)
    WHERE orderid=oid;


    IF TG_OP = 'UPDATE'or TG_OP='INSERT' THEN
        return NEW;

    ELSE --DELETE
        return OLD;

    END IF;

END;
$$ LANGUAGE plpgsql;

-- Ahora, creamos el trigger que llama a la función creada en los eventos INSERT, UPDATE y DELETE
CREATE TRIGGER updOrders
AFTER INSERT OR UPDATE OR DELETE ON orderdetail
FOR EACH ROW 
EXECUTE FUNCTION updOrders_trigger();