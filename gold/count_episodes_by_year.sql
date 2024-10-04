SELECT  YEAR(TO_DATE(dtLancamento, 'MMMM d, yyyy')) AS ano,
        COUNT(*) AS qtdEpisodios,
        CURRENT_TIMESTAMP() AS dtAtualizacao
FROM rickmorty_episode
GROUP BY YEAR(TO_DATE(dtLancamento, 'MMMM d, yyyy'))
