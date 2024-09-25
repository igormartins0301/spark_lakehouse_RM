SELECT 
    id as idEpisodio,
    EXPLODE(characters) AS urlPersonagem,
    updated_at as dtAtualizacao
FROM rickmorty_episode
