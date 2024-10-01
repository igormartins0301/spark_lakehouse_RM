WITH explodeTable AS(
    SELECT 
        id,
        EXPLODE(residents) AS residents,
        updated_at
    FROM rickmorty_location)
SELECT 
    id AS idLocal,
    REGEXP_EXTRACT(residents, r'/(\d+)$') AS idResidente,
    updated_at AS dtAtualizacao
FROM explodeTable

