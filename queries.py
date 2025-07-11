from neo4j import GraphDatabase 
from graphdatascience import GraphDataScience
import pandas as pd
import os
from dotenv import load_dotenv
from IPython.display import display
import warnings
warnings.filterwarnings('ignore')

# Crear una clase que contenga las queries de la base de datos
class DB_Queries:
    # Inicializar nuestra clase con las credenciales
    def __init__(self, database):
        load_dotenv()

        self.uri = os.getenv('URI')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
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
            degree_query = f"""CALL gds.degree.write('Undirected_{node}', {{writeProperty: 'Degree'}})
            YIELD centralityDistribution, nodePropertiesWritten
            RETURN centralityDistribution.min AS minimumScore, 
            centralityDistribution.mean AS meanScore, 
            nodePropertiesWritten;
            """
            self.session.run(degree_query)
            print("Degree Attribute added Successfully!")

            # In Degree
            inDegree_query = f"""CALL gds.degree.write('Reversed_{node}', {{writeProperty: 'In_Degree'}})
            YIELD centralityDistribution, nodePropertiesWritten
            RETURN centralityDistribution.min AS minimumScore, 
            centralityDistribution.mean AS meanScore, 
            nodePropertiesWritten;
            """
            self.session.run(inDegree_query)
            print("In_Degree Attribute added Successfully!")

            # Out Degree
            outDegree_query = f"""CALL gds.degree.write('Directed_{node}', {{writeProperty: 'Out_Degree'}} )
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
            {{writeProperty: 'Page_Rank'}} )
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
            {{writeProperty: 'Betweenness' }} )
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
            {{writeProperty: 'Closeness_Centrality'}} )
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
            {{writeProperty: 'Louvain'}} )
            YIELD communityCount, modularity, modularities;
            """
            self.session.run(louvain_query)
            print("Louvain Attribute added Successfully!")

            # Label Propagation
            lp_query = f"""CALL gds.labelPropagation.write(
            'Directed_{node}', 
            {{writeProperty: 'Label_Propagation'}} )
            YIELD communityCount, ranIterations, didConverge;
            """
            self.session.run(lp_query)
            print("Label Propagation Attribute added Successfully!")

            # Triangle Count
            triangle_query = f"""CALL gds.triangleCount.write(
            'Undirected_{node}',
            {{writeProperty: 'Triangles'}} )
            YIELD globalTriangleCount, nodeCount;
            """
            self.session.run(triangle_query)
            print("Triangle Count Attribute added Successfully!")

            # Local Clustering Coefficient
            lcc_query = f"""CALL gds.localClusteringCoefficient.write(
            'Undirected_{node}', 
            {{writeProperty: 'localClusteringCoefficient'}} )
            YIELD averageClusteringCoefficient, nodeCount;
            """
            self.session.run(lcc_query)
            print("Local Clustering Coefficient Attribute added Successfully!")

            # SCC
            scc_query = f"""CALL gds.scc.write(
            'Directed_{node}', {{writeProperty: 'Community_SCC'}} )
            YIELD componentCount, componentDistribution;
            """
            self.session.run(scc_query)
            print("Strongly CC Attribute added Successfully!")

            # WCC
            wcc_query = f"""CALL gds.wcc.write(
            'Undirected_{node}', {{writeProperty: 'Community_WCC'}} )
            YIELD componentCount, componentDistribution;
            """
            self.session.run(wcc_query)
            print("Weakly CC Attribute added Successfully!")

        except Exception as e:
            return f'An error occured: {e}'  

    def delta_pathing(self, node, attr, source, weight):
        try:
            orientations = ['Directed', 'Undirected']
            fix_source  = source.replace(" ", "_")

            for dir in orientations: 
                test = f"""MATCH (n:{node} {{ {attr}: '{source}' }}) -[r:PATH_{fix_source}_{dir}]-> (m:{node})
                RETURN n.{attr}, r.nodeIds, r.costs, r.totalCost, m.{attr}
                LIMIT 3;
                """

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
                print(self.gds.run_cypher(test))
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
                WHERE id(v) IN r.nodeIds                                                          // Encontrar los villanos que pertenecen a la red
                RETURN m.{attr} AS Source, COLLECT(v.{attr}) AS Bridges, 
                r.costs AS Partial_Costs, r.totalCost AS Total_Cost, n.{attr} AS Target;
                """

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
                display(self.gds.run_cypher(test))
                print()

        except Exception as e:
            return f'An error occured: {e}'

    def close(self):
        self.session.close()
        self.driver.close()