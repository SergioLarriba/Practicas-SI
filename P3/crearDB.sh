export PGUSER=alumnodb
export PGPASSWORD=1234
export PGDATABASE=si1

dropdb si1
createdb -U alumnodb si1
gunzip -c dump_v1.6-P3.sql.gz | psql -U alumnodb si1
