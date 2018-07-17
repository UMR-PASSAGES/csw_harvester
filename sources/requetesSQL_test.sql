TRUNCATE contact,dataidentification,extraction,geographicboundingbox,keyword,metadata,responsibleparty,sdi CASCADE;


SELECT s.id_sdi,s.name, Count(e.id_metadata)
FROM extraction e JOIN sdi s USING(id_sdi)
GROUP BY s.id_sdi
ORDER BY id_sdi

