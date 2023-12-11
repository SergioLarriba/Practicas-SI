
-----------------------------------------------------------------
---------------AÑADIDO-------------------------------------------
-----------------------------------------------------------------


--SELECT orderid, COUNT(*)
--FROM orderdetail
--GROUP BY orderid
--HAVING COUNT(*) > 1;


--ALTER TABLE ONLY orderdetail
--    ADD CONSTRAINT orderdetail_pkey PRIMARY KEY (orderid);






----------------------------MOVIES-------------------------




--ALTER TABLE imdb_movies
--ADD CONSTRAINT pk_movies_movieid
--PRIMARY KEY (movieid);

--Añadir dos campos a la tabla imdb_movies, para contener la valoración media
--ratingmean y el número de valoraciones ratingcount, de cada película


ALTER TABLE ONLY imdb_movies
ADD COLUMN ratingmean numeric DEFAULT 0;

ALTER TABLE ONLY imdb_movies
ADD COLUMN ratingcount integer DEFAULT 0;


-----------------ACTORMOVIES--------------------------




SELECT 
    conname AS constraint_name 
FROM 
    pg_constraint 
WHERE 
    conrelid = 'imdb_actormovies'::regclass AND 
    contype = 'p';




-- In imdb_actormovies table
ALTER TABLE imdb_actormovies
ADD CONSTRAINT imdb_actormovies_actorid_fkey 
FOREIGN KEY (actorid) 
REFERENCES imdb_actors (actorid);

ALTER TABLE imdb_actormovies
ADD CONSTRAINT imdb_actormovies_movieid_fkey 
FOREIGN KEY (movieid) 
REFERENCES imdb_movies (movieid);


-----------------MOVIECOUNTRIES--------------------

CREATE TABLE countries(
    countryid serial PRIMARY KEY,
    countryName VARCHAR(150) NOT NULL UNIQUE
);

ALTER TABLE ONLY imdb_moviecountries
    ADD COLUMN countryid integer not null default 0;
-- Asegurarse de que cada película en imdb_moviecountries exista en imdb_movies
ALTER TABLE imdb_moviecountries
ADD CONSTRAINT fk_moviecountries_movieid
FOREIGN KEY (movieid)
REFERENCES imdb_movies (movieid)
ON DELETE CASCADE
ON UPDATE CASCADE;


ALTER TABLE ONLY imdb_moviecountries
    ADD COLUMN countryName VARCHAR(150);

UPDATE imdb_moviecountries
SET countryId = countries.countryId
FROM countries
WHERE countries.countryName = imdb_moviecountries.countryName;


-----------------MOVIELANGUAGES--------------------------
-- Asegurarse de que cada película en imdb_movielanguages exista en imdb_movies
ALTER TABLE imdb_movielanguages
ADD CONSTRAINT fk_movielanguages_movieid
FOREIGN KEY (movieid)
REFERENCES imdb_movies (movieid)
ON DELETE CASCADE;

-----------------MOVIEGENRES-----------------
ALTER TABLE ONLY imdb_moviegenres
    ADD COLUMN genreId integer NOT NULL default 1;


DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indrelid = 'imdb_moviegenres'::regclass AND indisprimary
    ) THEN
        ALTER TABLE imdb_moviegenres
        DROP CONSTRAINT imdb_moviegenres_pkey;
    END IF;
END $$;

-- Primero, vamos a eliminar la columna genreId que se agregó anteriormente
ALTER TABLE imdb_moviegenres
DROP COLUMN genreId;

-- Luego, vamos a agregar la columna genreId de nuevo, pero esta vez con una secuencia para asegurar valores únicos
ALTER TABLE imdb_moviegenres
ADD COLUMN genreId SERIAL PRIMARY KEY;

-- Asegurarse de que cada película en imdb_moviegenres exista en imdb_movies
ALTER TABLE imdb_moviegenres
ADD CONSTRAINT fk_moviegenres_movieid
FOREIGN KEY (movieid)
REFERENCES imdb_movies (movieid)
ON DELETE CASCADE
ON UPDATE CASCADE;







----------------------INVENTORY----------------------------
ALTER TABLE inventory
    ADD CONSTRAINT inventory_prod_id_fkey 
    FOREIGN KEY (prod_id)
    REFERENCES products (prod_id);


----------------------ORDERS-------------------------------

ALTER TABLE orders
    ADD CONSTRAINT orders_customerid_fkey
    FOREIGN KEY (customerid)
    REFERENCES customers (customerid);


----------------------RATINGS------------------------------
--Una nueva tabla ratings para guardar las valoraciones que ha dado cada usuario
--a cada película, de forma que se evite que un mismo usuario pueda valorar dos
--veces la misma película.
CREATE TABLE ratings (
    user_id INT NOT NULL,
    movie_id INT NOT NULL,
    rating DECIMAL NOT NULL CHECK (rating >= 1 AND rating <= 5),
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (user_id) REFERENCES customers(customerid),
    FOREIGN KEY (movie_id) REFERENCES imdb_movies(movieid)
);

--------------------------ORDERDETAIL---------------------


	CREATE TABLE aux(
		orderid	integer NOT NULL,
		prod_id integer NOT NULL,
		price numeric,
		quantity integer NOT NULL
	);

	INSERT INTO aux(orderid, prod_id, price, quantity)
	SELECT orderid, prod_id, price, SUM(quantity)
	FROM orderdetail
	GROUP BY orderid, prod_id, price;


	DELETE FROM orderdetail;
	INSERT INTO orderdetail
	SELECT *
	FROM aux;
	DROP TABLE aux;



-- Create a unique index on (orderid, prod_id)
CREATE UNIQUE INDEX orderdetail_unique_idx ON orderdetail (orderid, prod_id);

    -- Si existe una restricción de clave primaria con el nombre 'orderdetail_pkey', elimínala
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'orderdetail_pkey'
        ) THEN
            ALTER TABLE ONLY orderdetail
            DROP CONSTRAINT orderdetail_pkey;
        END IF;
    END
    $$;

    -- Agrega la nueva restricción de clave primaria
    ALTER TABLE ONLY orderdetail
    ADD CONSTRAINT ordet PRIMARY KEY (orderid, prod_id);

    ALTER TABLE ONLY orderdetail
    ADD CONSTRAINT ordid FOREIGN KEY (orderid)
    REFERENCES orders (orderid)
    ON DELETE CASCADE
    ON UPDATE CASCADE;





------------------------CUSTOMERS-----------------------
--Un campo balance en la tabla customers, para guardar el saldo de los clientes.
ALTER TABLE customers
ADD COLUMN balance DECIMAL(10, 3) DEFAULT 0.000 NOT NULL;


--Auumentar customers a 96
ALTER TABLE customers
ALTER COLUMN password TYPE CHAR(96);


-- Crear procedimiento que inicializa balance (0,N)
CREATE OR REPLACE FUNCTION setCustomersBalance(IN initialBalance bigint)
RETURNS void AS $$
BEGIN
    UPDATE customers
    SET balance = floor(random() * (initialBalance + 1));
END;
$$ LANGUAGE plpgsql;



-- Llamar al procedimiento para establecer un balance aleatorio hasta 200
SELECT setCustomersBalance(200);










--APARTADO F, ELIMINAR ATRIBUTOS MULTIVALUADOS

CREATE TABLE languages(
    languageid serial PRIMARY KEY,
    languageName VARCHAR(150) NOT NULL UNIQUE
);

INSERT INTO languages (languagename)
SELECT DISTINCT language
FROM imdb_movielanguages
ON CONFLICT (languagename) DO NOTHING;


INSERT INTO countries (countryName)
SELECT DISTINCT countryid
FROM imdb_moviecountries
ON CONFLICT (countryName) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns 
        WHERE table_name = 'imdb_moviecountries' AND column_name = 'countryid'
    ) THEN
        ALTER TABLE ONLY imdb_moviecountries
        ADD COLUMN countryid INTEGER;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints 
        WHERE table_name = 'imdb_moviecountries' AND constraint_name = 'fk_moviecountries_movieid'
    ) THEN
        ALTER TABLE imdb_moviecountries
        ADD CONSTRAINT fk_moviecountries_movieid 
        FOREIGN KEY (movieid) 
        REFERENCES imdb_movies (movieid);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns 
        WHERE table_name = 'imdb_moviecountries' AND column_name = 'countryname'
    ) THEN
        ALTER TABLE ONLY imdb_moviecountries
        ADD COLUMN countryname VARCHAR(150);
    END IF;
END $$;

INSERT INTO countries (countryName)
SELECT DISTINCT countryid
FROM imdb_moviecountries
ON CONFLICT (countryName) DO NOTHING;

ALTER TABLE ONLY imdb_moviecountries
    ADD CONSTRAINT moviecoun FOREIGN KEY (movieid)
    REFERENCES imdb_movies (movieid)
    ON DELETE CASCADE
    ON UPDATE CASCADE;

CREATE TABLE genres(
    genreid serial PRIMARY KEY,
    genreName VARCHAR(150) NOT NULL UNIQUE
);

INSERT INTO genres (genreName)
SELECT DISTINCT genre
FROM imdb_moviegenres
ON CONFLICT (genreName) DO NOTHING;






