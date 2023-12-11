
explain analyze select count(*)
from orders
where status is null;

explain analyze select count(*)
from orders
where status ='Shipped';

create index idx_status on orders(status);
--drop index idx_status;

explain analyze select count(*)
from orders
where status is null;

explain analyze select count(*)
from orders
where status ='Shipped';

analyze orders;


explain analyze select count(*)
from orders
where status is null;

explain analyze select count(*)
from orders
where status ='Shipped';

explain analyze select count(*)
from orders
where status ='Paid';

explain analyze select count(*)
from orders
where status ='Processed';


