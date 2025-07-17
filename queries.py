from neo4j import GraphDatabase 
from graphdatascience import GraphDataScience
import pandas as pd
import os
from dotenv import load_dotenv
from IPython.display import display
import warnings
import time
import itertools
pd.set_option('display.max_colwidth', None)
warnings.filterwarnings('ignore')

# Crear una clase que contenga las queries de la base de datos
class DB_Queries:
    # Inicializar nuestra clase con las credenciales
    def __init__(self, database='neo4j'):
        #load_dotenv()

        self.uri = 'bolt://localhost:7687'
        self.user = 'neo4j'
        self.password = '123456789'
        # Autentificar
        self.driver = GraphDatabase.driver(self.uri, auth = (self.user, self.password))
        self.database = database
           
        try: 
            self.driver.verify_connectivity()           # Comprobar la conexión
            print(f'Successfully Connected to Neo4j in {self.database} database!')
        except Exception as e:
            return print(f'Connection failed to Neo4j: {e}')
        
        # Establecer la conexión con GraphDataScience
        try: 
            self.gds = GraphDataScience(self.uri, auth=(self.user, self.password))
            self.gds.set_database(self.database)
            test = self.gds.run_cypher('RETURN 1 AS Prueba')
            if test['Prueba'].iloc[0] == 1:
                print(f'Successfully Connected to GraphDataScience in {self.database} database!')
            else: 
                print('Error connecting to GraphDataScience.')
        except Exception as e:
            return print(f'Connection failed to GraphDataScience: {e}')

        # Inicializar sesión
        self.session = self.driver.session(database=self.database)

    def custom_query(self, query):
        try:
            return self.gds.run_cypher(query)
        except Exception as e:
            return print(f'Error with query: {e}')

    def count_nodes(self, node):
        query = f""" MATCH (n:{node})
        RETURN COUNT(DISTINCT(n)) AS {node}s
        """

        return self.gds.run_cypher(query)

    def node_attributes(self):
        query ='CALL db.schema.nodeTypeProperties'
        return self.gds.run_cypher(query)

    def count_relationships(self, rel):
        query = f"""MATCH () -[r:{rel}]-> ()
        RETURN COUNT(DISTINCT(r)) as {rel}_Count;
        """

        return self.gds.run_cypher(query)
    
    def rel_attributes(self):
        query ='CALL db.schema.relTypeProperties'
        return self.gds.run_cypher(query)

    def find_attributes(self, node, attributes, limit=10):
        try:
            # Contabilizar cuántos elementos hay en la lista 
            if len(attributes) == 1:                      # Solo 1 Atributo 
                query = f""" MATCH (n:{node})
                RETURN n.{attributes[0]} 
                LIMIT {limit}
                """
            elif len(attributes) > 1:                     # Más de 1 Atributo
                attributes_str = ','.join([f'n.{i} AS {i}' for i in attributes])
                query = f""" MATCH (n:{node})
                RETURN {attributes_str} 
                LIMIT {limit}
                """
            else:
                return print('Invalid Attribute Number')

            # Ejecutar la query
            rows = None
            rows = self.gds.run_cypher(query)
            return rows
        
        except Exception as e:
            print(f'An error occured {e}')

    def create_subgraph(self, node, rel, orientation, weight=None):
        try: 
            # En caso de que hayan pesos o no en la relación
            if weight: 
                rel_block = f" {{ {rel}: {{orientation: '{orientation}', properties: '{weight}' }} }} "
            else:
                rel_block = f" {{ {rel}: {{orientation: '{orientation}'}} }} "

            name = 'myGraph'
            if orientation == 'NATURAL':
                name = 'Directed'
            elif orientation == 'REVERSE':
                name = 'Reversed'
            elif orientation ==  'UNDIRECTED':
                name = 'Undirected'
            else:
                return f"Invalid orientation: {orientation}"

            graph_name = f'{name}_{node}'

            # Checar si existe el subgrafo
            exists_query = f"CALL gds.graph.exists('{graph_name}') YIELD exists"
            result = self.session.run(exists_query)
            exists = result.single()["exists"]

            # Saltar la creación en caso de que el subgrafo exista
            if exists: 
                return f'Subgraph {graph_name} already exists. Creation skipped.'

            # Crearlo en caso de que el subgrafo NO exista
            query = f"""CALL gds.graph.project(
            '{graph_name}',      
            '{node}',
            {rel_block})
            """
            self.session.run(query)
            return print(f'Successfully created {name} Subgraph with {node} nodes and {rel} relationships!')
        
        except Exception as e:
            return f'An error occured {e}'
        
    def create_subgraph_continent(self, node, rel,continent):
        try: 
            
            graph_name = f'{node}_{continent}'

            # Checar si existe el subgrafo
            exists_query = f"CALL gds.graph.exists('{graph_name}') YIELD exists"
            result = self.session.run(exists_query)
            exists = result.single()["exists"]

            # Saltar la creación en caso de que el subgrafo exista
            if exists: 
                return f'Subgraph {graph_name} already exists. Creation skipped.'

            # Crearlo en caso de que el subgrafo NO exista
            query = f"""
            MATCH (a:Airport {{continent: '{continent}'}})-[:TO]->(a2:Airport {{continent: '{continent}'}})
            WITH gds.graph.project('{graph_name}',a,a2) AS g
            RETURN 
                g.graphName, g.nodeCount, g.relationshipCount;
            """
            self.session.run(query)
            return print(f'Successfully created {graph_name} Subgraph with {node} nodes and {rel} relationships!')
        
        except Exception as e:
            return f'An error occured {e}'

    def check_subgraphs(self):
        query = """CALL gds.graph.list 
        YIELD graphName, nodeCount, relationshipCount, database, creationTime;
        """
        rows = None
        rows = self.gds.run_cypher(query)
        return rows

    def degrees(self,node):
        try:
            # Degree
            degree_query = f"""CALL gds.degree.write('Undirected_{node}', {{writeProperty: 'Degree_{node}'}})
            YIELD centralityDistribution, nodePropertiesWritten
            RETURN centralityDistribution.min AS minimumScore, 
            centralityDistribution.mean AS meanScore, 
            nodePropertiesWritten;
            """
            self.session.run(degree_query)
            print("Degree Attribute added Successfully!")

            # In Degree
            inDegree_query = f"""CALL gds.degree.write('Reversed_{node}', {{writeProperty: 'In_Degree_{node}'}})
            YIELD centralityDistribution, nodePropertiesWritten
            RETURN centralityDistribution.min AS minimumScore, 
            centralityDistribution.mean AS meanScore, 
            nodePropertiesWritten;
            """
            self.session.run(inDegree_query)
            print("In_Degree Attribute added Successfully!")

            # Out Degree
            outDegree_query = f"""CALL gds.degree.write('Directed_{node}', {{writeProperty: 'Out_Degree_{node}'}} )
            YIELD centralityDistribution, nodePropertiesWritten
            RETURN centralityDistribution.min AS minimumScore, 
            centralityDistribution.mean AS meanScore, 
            nodePropertiesWritten;
            """
            self.session.run(outDegree_query)
            print("Out_Degree Attribute added Successfully!")

        except Exception as e:
            return f'An error occured: {e}'

    def page_rank(self, node):
        try:
            query = f"""CALL gds.pageRank.write(
            'Directed_{node}',
            {{writeProperty: 'Page_Rank_{node}'}} )
            YIELD nodePropertiesWritten, ranIterations;
            """
            self.session.run(query)
            print('Page Rank Attribute added Successfully!')

        except Exception as e:
            return f'An error occured: {e}'            

    def betwenness(self,node):
        try:
            query = f"""CALL gds.betweenness.write(
            'Directed_{node}', 
            {{writeProperty: 'Betweenness_{node}' }} )
            YIELD centralityDistribution, nodePropertiesWritten;
            """
            self.session.run(query)
            print('Betweenness Attribute added Successfully!')

        except Exception as e:
            return f'An error occured: {e}'  
        
    def closeness(self,node):
        try:
            query = f"""CALL gds.closeness.write(
            'Directed_{node}',                       
            {{writeProperty: 'Closeness_Centrality_{node}'}} )
            YIELD nodePropertiesWritten, centralityDistribution;
            """
            self.session.run(query)
            print('Closeness Attribute added Successfully!')

        except Exception as e:
            return f'An error occured: {e}'  

    def community_algorithms(self, node):
        try:
            # Louvain
            louvain_query = f"""CALL gds.louvain.write(
            'Directed_{node}',
            {{writeProperty: 'Louvain_{node}'}} )
            YIELD communityCount, modularity, modularities;
            """
            self.session.run(louvain_query)
            print("Louvain Attribute added Successfully!")

            # Label Propagation
            lp_query = f"""CALL gds.labelPropagation.write(
            'Directed_{node}', 
            {{writeProperty: 'Label_Propagation_{node}'}} )
            YIELD communityCount, ranIterations, didConverge;
            """
            self.session.run(lp_query)
            print("Label Propagation Attribute added Successfully!")

            # Triangle Count
            triangle_query = f"""CALL gds.triangleCount.write(
            'Undirected_{node}',
            {{writeProperty: 'Triangles_{node}'}} )
            YIELD globalTriangleCount, nodeCount;
            """
            self.session.run(triangle_query)
            print("Triangle Count Attribute added Successfully!")

            # Local Clustering Coefficient
            lcc_query = f"""CALL gds.localClusteringCoefficient.write(
            'Undirected_{node}', 
            {{writeProperty: 'localClusteringCoefficient_{node}'}} )
            YIELD averageClusteringCoefficient, nodeCount;
            """
            self.session.run(lcc_query)
            print("Local Clustering Coefficient Attribute added Successfully!")

            # SCC
            scc_query = f"""CALL gds.scc.write(
            'Directed_{node}', {{writeProperty: 'Community_SCC_{node}'}} )
            YIELD componentCount, componentDistribution;
            """
            self.session.run(scc_query)
            print("Strongly CC Attribute added Successfully!")

            # WCC
            wcc_query = f"""CALL gds.wcc.write(
            'Undirected_{node}', {{writeProperty: 'Community_WCC_{node}'}} )
            YIELD componentCount, componentDistribution;
            """
            self.session.run(wcc_query)
            print("Weakly CC Attribute added Successfully!")

            # K1-Coloring
            k1_color_query = f"""CALL gds.k1coloring.write(
            'Undirected_{node}',
            {{writeProperty: 'K1Color_{node}'}}
            )
            YIELD nodeCount, colorCount, ranIterations, didConverge
            """
            self.session.run(k1_color_query)
            print("K1-Coloring Attribute added Successfully!")

        except Exception as e:
            return f'An error occured: {e}'  

    def color_correction(self, node):
        try:
            algorithms = ['Louvain', 'Label_Propagation', 'Triangles', 'localClusteringCoefficient',
                          'Community_SCC', 'Community_WCC', 'K1Color']
            for i in algorithms:
                query = f"""MATCH (n:{node})
                SET n.{i}_{node} = n.{i}_{node} + 1
                """
                self.session.run(query)
                print(f"{i} Attribute Color Corrected Successfully!")
        except Exception as e:
            return f'An error occured: {e}'

    def delta_pathing(self, node, attr, source, weight):
        try:
            orientations = ['Directed', 'Undirected']
            fix_source  = source.replace(" ", "_")

            for dir in orientations: 
                test = f"""MATCH (n:{node} {{ {attr}: '{source}' }}) -[r:PATH_{fix_source}_{dir}]-> (m:{node})
                MATCH (v:{node})
                WHERE id(v) IN r.nodeIds 
                RETURN n.{attr} AS Source, COLLECT(v.{attr}) AS Bridges, r.costs AS Partial_Costs, 
                r.totalCost AS Total_Cost, m.{attr} AS Target
                LIMIT 3;
                """

                # Checar si existe el camino
                exists_query = f"MATCH  (n:{node}) -[r:PATH_{fix_source}_{dir}]- (m:{node}) RETURN COUNT(r) > 0 AS exists"
                result = self.session.run(exists_query)
                exists = result.single()["exists"]

                # Saltar la creación en caso de que el camino exista
                if exists: 
                    print(f'{dir} Delta Pathing for {source} already exists. Creation skipped.')
                    display(self.gds.run_cypher(test))
                    continue

                query = f"""MATCH (source: {node}{{ {attr}: '{source}' }})
                CALL gds.allShortestPaths.delta.write('{dir}_{node}', {{
                    sourceNode: source,
                    relationshipWeightProperty: '{weight}',
                    writeRelationshipType: 'PATH_{fix_source}_{dir}',         
                    writeNodeIds: true,
                    writeCosts: true
                }})
                YIELD relationshipsWritten
                RETURN relationshipsWritten;
                """
                self.session.run(query)
                print(f'{dir} Delta Pathing for {source} added!')
                time.sleep(3)                                 # Esperar a que se añadan las relaciones
                display(self.gds.run_cypher(test))
                print()

        except Exception as e:
            return f'An error occured: {e}'

    def dijkstra(self, node, attr, source, target, weight):
        try:
            orientations = ['Directed', 'Undirected']
            fix_source  = source.replace(" ", "_")
            fix_target  = target.replace(" ", "_")

            for dir in orientations: 
                test = f"""MATCH (m) -[r:Dijkstra_{fix_source}_to_{fix_target}_{dir}]-> (n)             // Encontrar todos los nodos con Dijkstra
                MATCH (v:{node})
                WHERE id(v) IN r.nodeIds                                                                // Encontrar los nodos que pertenecen a la red
                RETURN m.{attr} AS Source, COLLECT(v.{attr}) AS Bridges, 
                r.costs AS Partial_Costs, r.totalCost AS Total_Cost, n.{attr} AS Target;
                """

                # Checar si existe el camino
                exists_query = f"MATCH  (n:{node}) -[r:Dijkstra_{fix_source}_to_{fix_target}_{dir}]- (m:{node}) RETURN COUNT(r) > 0 AS exists"
                result = self.session.run(exists_query)
                exists = result.single()["exists"]

                # Saltar la creación en caso de que el camino exista
                if exists: 
                    print(f'{dir} Dijkstra Pathing for {source} to {target} already exists. Creation skipped.')
                    display(self.gds.run_cypher(test))
                    continue

                query = f"""MATCH (source: {node} {{ {attr}: '{source}'}} ),
                (target: {node} {{ {attr}: '{target}'}} )
                CALL gds.shortestPath.dijkstra.write('{dir}_{node}', {{
                    sourceNode: source,
                    targetNodes: target,
                    relationshipWeightProperty: '{weight}',
                    writeRelationshipType: 'Dijkstra_{fix_source}_to_{fix_target}_{dir}',
                    writeNodeIds: true,
                    writeCosts: true
                }})
                YIELD relationshipsWritten
                RETURN relationshipsWritten;
                """
                self.session.run(query)
                print(f'{dir} Dijkstra Pathing for {source} to {target} added!')
                time.sleep(3)
                display(self.gds.run_cypher(test))
                print()

        except Exception as e:
            return f'An error occured: {e}'

    def temp_dijkstra_directed(self, node, attr, source, target, weight):
        try:
            query = f"""MATCH (source: {node} {{ {attr}: '{source}'}} ),
            (target: {node} {{ {attr}: '{target}'}} )
            CALL gds.shortestPath.dijkstra.stream('Directed_Airport', {{
                sourceNode: source,
                targetNodes: target,
                relationshipWeightProperty: '{weight}'
            }})
            YIELD sourceNode, targetNode, totalCost, nodeIds
            UNWIND nodeIds as id
            MATCH (n: {node}) WHERE id(n) = id
            WITH sourceNode, targetNode, totalCost, COLLECT(n.{attr}) AS Bridges
            RETURN gds.util.asNode(sourceNode).{attr} AS Source, 
            gds.util.asNode(targetNode).{attr} AS Target, Bridges, totalCost AS Distance;
            """
            return self.gds.run_cypher(query)

        except Exception as e:
            return pd.DataFrame(columns=['Source', 'Target', 'Bridges', 'Distance'])
        
    def seven_wonders_best_route(self, node, attr, starting_point, weight):        
        try:
            # Establecer los aeropuertos destino que contengan las 7 maravillas
            seven_wonders = ['Cairo', 'Rome', 'Beijing', 'Merida', 'Cuzco', 'Lucknow', 'Rio De Janeiro']
            # Realizar pares para ejecutar dijkstra 
            routes = seven_wonders + [starting_point]
            pairs = list(itertools.permutations(routes,2))

            # Calcular distancias entre origen y las 7 maravillas
            origin_to_wonder = pd.DataFrame(columns=['Source','Target','Middle','Distance'])

            # Separar por origen y destino
            for src, tgt in pairs:
                #Ejecutar el algoritmo y almacenar la respuesta
                answer = self.temp_dijkstra_directed(node,attr,source=src,target=tgt, weight=weight)
                origin_to_wonder = pd.concat([origin_to_wonder,answer],sort=False, ignore_index=True)
            
            # Diccionario para buscar los orígenes y destinos 
            lookup = {(row.Source, row.Target): {'Distance': row.Distance, 'Bridges': row.Bridges} 
                      for row in origin_to_wonder.itertuples(index=False)}

            # Todas las posibles rutas hacia las 7 maravillas
            permuts = list(itertools.permutations(seven_wonders))
            full_routes = [(starting_point,) + route for route in permuts]

            # Calcular las distancias
            distances = pd.DataFrame(columns=['Order','Distance', 'Full_route'])

            for route in full_routes:
                total_distance = 0
                full_route_list = [starting_point]
                valid = True

                for i in range(len(route) - 1):
                    pair = (route[i], route[i+1])
                    info = lookup.get(pair)
                    if info is None:
                        print(f"Missing distance for pair: {pair}")
                        valid = False
                        break
                    total_distance += info['Distance']

                    # Obtener los puentes
                    bridges = info['Bridges']
                    # Evitar la duplicación y unir los puentes en una sola lista
                    if bridges and bridges[0] == full_route_list[-1]:
                        full_route_list.extend(bridges[1:])
                    else:
                        full_route_list.extend(bridges)
                    full_route_list.append(route[i+1])

                if valid:
                    temp_df = pd.DataFrame({
                        'Order': [route],
                        'Distance': [total_distance],
                        'Full_route': [full_route_list]
                    })
                    distances = pd.concat([distances, temp_df], ignore_index=True)
            
            # Regresar la mejor ruta
            distances = distances.sort_values('Distance', ignore_index=True)
            best_route = distances.head(1)
            return best_route
                            
        except Exception as e:
            return f'An error occured: {e}'

    def find_best_route(self, node, attr, starting_point, destinations=list, weight=str):        
        try:           
            # Realizar pares para ejecutar dijkstra 
            routes = destinations + [starting_point]
            pairs = list(itertools.permutations(routes,2))

            # Calcular distancias entre origen y las 7 maravillas
            origin_to_wonder = pd.DataFrame(columns=['Source','Target','Middle','Distance'])

            # Separar por origen y destino
            for src, tgt in pairs:
                #Ejecutar el algoritmo y almacenar la respuesta
                answer = self.temp_dijkstra_directed(node,attr,source=src,target=tgt, weight=weight)
                origin_to_wonder = pd.concat([origin_to_wonder,answer],sort=False, ignore_index=True)
            
            # Diccionario para buscar los orígenes y destinos 
            lookup = {(row.Source, row.Target): {'Distance': row.Distance, 'Bridges': row.Bridges} 
                      for row in origin_to_wonder.itertuples(index=False)}

            # Todas las posibles rutas hacia las 7 maravillas
            permuts = list(itertools.permutations(destinations))
            full_routes = [(starting_point,) + route for route in permuts]

            # Calcular las distancias
            distances = pd.DataFrame(columns=['Order','Distance', 'Full_route'])

            for route in full_routes:
                total_distance = 0
                full_route_list = [starting_point]
                valid = True

                for i in range(len(route) - 1):
                    pair = (route[i], route[i+1])
                    info = lookup.get(pair)
                    if info is None:
                        print(f"Missing distance for pair: {pair}")
                        valid = False
                        break
                    total_distance += info['Distance']

                    # Obtener los puentes
                    bridges = info['Bridges']
                    # Evitar la duplicación y unir los puentes en una sola lista
                    if bridges and bridges[0] == full_route_list[-1]:
                        full_route_list.extend(bridges[1:])
                    else:
                        full_route_list.extend(bridges)
                    full_route_list.append(route[i+1])

                if valid:
                    temp_df = pd.DataFrame({
                        'Order': [route],
                        'Distance': [total_distance],
                        'Full_route': [full_route_list]
                    })
                    distances = pd.concat([distances, temp_df], ignore_index=True)
            
            # Regresar la mejor ruta
            distances = distances.sort_values('Distance', ignore_index=True)
            best_route = distances.head(1)
            return best_route
                            
        except Exception as e:
            return f'An error occured: {e}'

    def close(self):
        self.session.close()
        self.driver.close()