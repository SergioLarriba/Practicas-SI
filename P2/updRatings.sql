DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'updratings') THEN
        DROP TRIGGER updRatings ON ratings;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION updateRatings()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
        UPDATE imdb_movies
        SET ratingmean = (
            SELECT COALESCE(AVG(rating), 0) FROM ratings WHERE movie_id = NEW.movie_id
        ),
        ratingcount = (
            SELECT COALESCE(COUNT(rating), 0) FROM ratings WHERE movie_id = NEW.movie_id
        )
        WHERE movieid = NEW.movie_id;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE imdb_movies
        SET ratingmean = (
            SELECT COALESCE(AVG(rating), 0) FROM ratings WHERE movie_id = OLD.movie_id
        ),
        ratingcount = (
            SELECT COALESCE(COUNT(rating), 0) FROM ratings WHERE movie_id = OLD.movie_id
        )
        WHERE movieid = OLD.movie_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER updRatings
AFTER INSERT OR UPDATE OR DELETE
ON ratings
FOR EACH ROW
EXECUTE PROCEDURE updateRatings();