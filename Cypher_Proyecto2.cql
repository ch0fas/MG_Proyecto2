// Código Cypher - Proyecto 2
// Sofía Maldonado García
// Viviana Toledo De la Fuente
// César Adrián Santos Santacruz
// 17/07/2025

// 4.4
MATCH (a:Airport{Label:'Villahermosa'})-[*3]-(m)
RETURN m;