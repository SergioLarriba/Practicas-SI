MATCH (a1:Actor)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(a2:Actor)
WHERE a1.name <> "Winston, Hattie" AND a2.name <> "Winston, Hattie"
RETURN DISTINCT a1.name AS Actor
ORDER BY a1.name ASC
LIMIT 10