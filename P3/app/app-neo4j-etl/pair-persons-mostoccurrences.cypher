MATCH (p1:Person)-[:ACTED_IN|DIRECTED]->(:Movie)<-[:ACTED_IN|DIRECTED]-(p2:Person)
WHERE p1 <> p2
WITH p1, p2, count(*) AS occurrences
RETURN p1.name AS Person1, p2.name AS Person2, occurrences
ORDER BY occurrences DESC