WITH explodeTable AS(
    SELECT 
        id,
        EXPLODE(characters) AS character,
        updated_at
    FROM rickmorty_episode)
SELECT 
    id AS idEpisodio,
    REGEXP_EXTRACT(character, r'/(\d+)$') AS idPersonagem,
    updated_at AS dtAtualizacao
FROM explodeTable