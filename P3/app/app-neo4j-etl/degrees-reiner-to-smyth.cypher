MATCH (d:Director {name: "Reiner, Carl"}), (a:Actor {name: "Smyth, Lisa (I)"})
MATCH path = shortestPath((d)-[*]-(a))
RETURN path
