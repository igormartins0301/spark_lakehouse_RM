SELECT  idLocal,
        COUNT(idResidente) AS qtdResidentes,
        current_timestamp() AS dtAtualizacao
FROM rickmorty_location_residents
GROUP BY idLocal