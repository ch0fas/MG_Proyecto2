// Código Cypher - Proyecto 2
// Sofía Maldonado García
// Viviana Toledo De la Fuente
// César Adrián Santos Santacruz
// 17/07/2025

// 4.4
MATCH (a:Airport{Label:'Villahermosa'})-[*3]-(m)
RETURN m;

//4.6
MATCH (a:Airport)
WHERE a.country = 'Colombia'
RETURN count(DISTINCT a) AS total_aeropuertos;


MATCH (a:Airport)
WHERE a.country = 'Colombia'
RETURN 
  a.Label, 
  a.Betweenness, 
  a.Closeness_Centrality, 
  a.Page_Rank, 
  a.Degree,
  (a.Betweenness + a.Page_Rank + a.Degree) AS total_centrality
ORDER BY total_centrality DESC;

//4.7
MATCH (a) -[r:Dijkstra_bogota_to_Mexico]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

//preguntas de americas tour

//pregunta betwenneess
MATCH (a:Airport)
WHERE a.continent = 'America'
RETURN 
  a.Label, 
  a.Betweenness, 
  a.Closeness_Centrality, 
  a.Page_Rank, 
  a.Degree,
  (a.Betweenness + a.Page_Rank + a.Degree) AS total_centrality
ORDER BY total_centrality DESC;
//pregunta del in degree
MATCH (a:Airport)<-[:VUELO]-()
WHERE a.country IN ['United States', 'Canada']
RETURN 
  a.Label AS aeropuerto,
  a.codigo AS codigo,
  a.country AS pais,
  count(*) AS in_degree
ORDER BY in_degree DESC
LIMIT 10;
//pregunta 10 con más degree sin estados unidos ni canada 
MATCH (a:Airport)
WHERE NOT a.country IN ['United States', 'Canada']
RETURN 
  a.Label AS pais,
  a.degree AS degree,
ORDER BY degree DESC
LIMIT 5;

  MATCH (source: Airport {Label: 'Guadalajara'}),
(target: Airport {Label: 'Mexico City'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_GDL_TO_CDMX',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_GDL_TO_CDMX]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

//la distancia con un vuelo directo son 	459.6216376140589


MATCH (source: Airport {Label: 'Mexico City'}),
(target: Airport {Label: 'Cancun'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_CDMX_TO_CANCUN',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_CDMX_TO_CANCUN]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

// 1285.7973824809449

MATCH (source: Airport {Label: 'Cancun'}),
(target: Airport {Label: 'Panama'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_CANCUN_TO_PANAMA',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_CANCUN_TO_PANAMA]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

//pasamos por el aeropuerto San Pedro Sula  y la distancia es de 1908.3213341804592


MATCH (source: Airport {Label: 'Panama'}),
(target: Airport {Label: 'Bogota'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_PANAMA_TO_BOGOTA',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_PANAMA_TO_BOGOTA]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

// la distancia es de 1782.8964779420646

MATCH (source: Airport {Label: 'Bogota'}),
(target: Airport {Label: 'Sao Paulo'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_BOGOTA_TO_SAO_PAULO',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_BOGOTA_TO_SAO_PAULO]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;


//la distancia es de 4340.7437556029345

//para regresar hay un vuelo de sao paulo a ciudad de mexico 

MATCH (source: Airport {Label: 'Sao Paulo'}),
(target: Airport {Label: 'Mexico City'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_SAO_PAULO_TO_CDMX',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_SAO_PAULO_TO_CDMX]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

//la distancia siendo de 7441 mil kilometros


//para sacar los aeropuertos con mayor Betweenness
MATCH (a:Airport)
WHERE a.continent = 'America'
RETURN 
  a.Label, 
  a.Betweenness, 
  a.Closeness_Centrality, 
  a.Page_Rank, 
  a.Degree,
  (a.Betweenness + a.Page_Rank + a.Degree) AS total_centrality
ORDER BY total_centrality DESC;


//paraa sacar los aeropuertos con mayor in degree
MATCH (a:Airport)<-[:TO]-()
WHERE a.country IN ['United States', 'Canada']
RETURN 
  a.Label AS aeropuerto,
  a.country AS pais,
  count(*) AS in_degree
ORDER BY in_degree DESC
LIMIT 10;



//dijikstra a estados unidos
MATCH (source: Airport {Label: 'Mexico City'}),
(target: Airport {Label: 'Atlanta'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_CDMX_TO_ATLANTA',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_CDMX_TO_ATLANTA]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

// la distancia es de 2145.796614774282 km


MATCH (source: Airport {Label: 'Atlanta'}),
(target: Airport {Label: 'Chicago'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_ATLANTA_TO_CHICAGO',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

// distancia de 953.0273915894652

MATCH (source: Airport {Label: 'Chicago'}),
(target: Airport {Label: 'New York'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_CHICAGO_TO_NY',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

//distancia de 1165.23566438093

MATCH (source: Airport {Label: 'New York'}),
(target: Airport {Label: 'Los Angeles'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_NY_TO_LA',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_NY_TO_LA]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;


//distancia de 3233.24567846

MATCH (source: Airport {Label: 'Los Angeles'}),
(target: Airport {Label: 'Mexico City'})
CALL gds.shortestPath.dijkstra.write('DIRECTED', {
    sourceNode: source,
    targetNodes: target,
    relationshipWeightProperty: 'distance',
    writeRelationshipType: 'Dijkstra_LA_TO_CDMX',
    writeNodeIds: true,
    writeCosts: true
})
YIELD relationshipsWritten
RETURN relationshipsWritten;

MATCH (a) -[r:Dijkstra_LA_TO_CDMX]->(b)
RETURN a.Label, r.nodeIds, r.costs, r.totalCost, b.Label;

//la distancia de regreso de estados unidos a mexico es de 2502.8032819219748