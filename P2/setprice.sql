
with PrecioAjustado as(
    --selecciono el ID del pedido y del producto
    select 
        od.orderid,
        od.prod_id,
        --Calculo precio dividiendo precio actual / factor de aumento acumulado (2% anual)
        p.price / power(1.02,extract(year from now()) - extract(year from o.orderdate)) as precioNuevo
    from
        orderdetail as od
        --join con tabla pedidos para obtener fecha
        inner join orders as o on od.orderid = o.orderid
        --join con tabla productos para obtener precio actual
        inner join products as p on od.prod_id = p.prod_id
)
--Actualizamos tabla
update orderdetail
--Actualizamos precio
set price = q.precioNuevo

from PrecioAjustado as q
where
    --Actualizamos filas que corresponden a los IDs de pedido y producto calculados
    orderdetail.orderid = q.orderid AND orderdetail.prod_id = q.prod_id