from neo4j import GraphDatabase
#from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Datos de conexión 
URI = "bolt://localhost:7687"  
AUTH = ("neo4j", "DBMSPassword") 

def test_connection(): #Metodo extraido de la pagina de Neo4j for Python para verificar la conexion
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        
        with driver.session() as session:
            result = session.run("RETURN 'Conexión exitosa' AS message")
            record = result.single()
            print(record["message"])
            
        driver.close()
        return True
    
    except Exception as e:
        logger.error(f"Neo4j connection error: {e}")
        return False
if test_connection():
    print("Connection succesfull")
else:
    print("Check error details above")


def parametros_medios():
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

            #obtener las 30 ultimas canciones del historial
            records = session.run("match (u:User) -[:LISTENED_TO]-> (t:Track) where u.id = '1' LIMIT 30 return t")
            #imprimir los parametros de la cancion


            #crea la lista con valores
            features = [

                'acousticness',
                'danceability',
                'energy',
                'instrumentalness',
                'valence',
                'avg_tempo'
            ]

            #crea un diccionario para cada atributo segun la lista features   
            feature_values = {feature: [] for feature in features}

            #imprime cada cancion del historial
            for record in records:
                track_node = record["t"]
                print(track_node["name"])

            # Guarda cada atributo en el diccionario
                for feature in features:
                    if feature in track_node:
                        feature_values[feature].append(track_node[feature])
                    else:#Si no lo encuentra avisa que ese dato no se hallo
                        print(f"Error: El atributo {feature} no se encontro en la cancion {track_node['name']}")

                # Calcula el promedio de cada atributo en el diccionario
                averages = {}
                for feature, values in feature_values.items():
                    if values:  #Solo calcula si encuentra valores
                        averages[feature] = np.mean(values)
                    else:
                        averages[feature] = None
                
                
                return averages
            
            
    except Exception as e:
        logger.error(f"Error en la consulta: {e}")



def preferencias():

    #crea un diccionario vacio para guardar las preferencias del usuario
    preferencias = {
        "favorite_genre": None,
        "favorite_artist": None,
        "disliked_genres": None,
        "tempo_range": {"min": None, "max": None}
    }

    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:
            #genero
            result = session.run("MATCH (u:User)-[r:LIKES_GENRE]->(g) where u.id = '1' RETURN  g.name ")
            if result.peek():
                preferencias["favorite_genre"] = result.single()["g.name"]

            #artista
            result = session.run("MATCH (u:User)-[:FOLLOWS]->(a) where u.id = '1' return a.name")
            if result.peek():
                preferencias["favorite_artist"] = result.single()["a.name"]

            #genero "disliked"
            result = session.run("MATCH (u:User)-[r:DISLIKES_GENRE]->(g) where u.id = '1' RETURN  g.name ")
            if result.peek():
                preferencias["disliked_genres"] = result.single()["g.name"]

            result = session.run("match (u:User) where u.id = '1' return u.tempo_max , u.tempo_min")
            if result.peek():
                record = result.single()
                preferencias["tempo_range"] = {
                    "min": record["u.tempo_min"],
                    "max": record["u.tempo_max"]
                }

            return preferencias

    except Exception as e:
        logger.error(f"Error en la consulta: {e}")




def rama1():

    tracks_data = [] #crea una lista para guardar las canciones
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

             #este query selecciona el historial del usuario y de ahi busca canciones en las que:
             #danceability, energy y valence 
             #tengan una diferencia menor que 5 con alguna cancion del autor
            records = session.run("match (u:User)-[:LISTENED_TO]->(t1:Track) where u.id = '1' "
            "with t1 LIMIT 20 MATCH (t2:Track) where t1 <> t2 and "
            "abs(t1.danceability - t2.danceability) < 0.035 and "
            "abs(t1.energy - t2.energy) < 0.035 and "
            "abs(t1.valence - t2.valence) < 0.035 "
            "return  distinct t2.id AS track_id, t2.danceability as dance, t2.energy AS energy, t2.valence as valence LIMIT 250")

            
            for record in records:
                track_id = record["track_id"] #guarda el id de la cancion 
                parametros = [record["dance"], record["energy"], record["valence"]] #guarda los parametros que se utilizan en una lista
                tracks_data.append((track_id, parametros)) #guarda la tupla lista, parametros
            

        return tracks_data #retorna el set

    except Exception as e:
        logger.error(f"Error en la consulta: {e}")

def rama2():

    tracks_data = [] #crea una lista para guardar las canciones
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:


            records = session.run("match (u:User)-[:FOLLOWS]->(a:Artist)-[:CREATED]-(t1:Track) where u.id = '1' " 
            "with t1 limit 20 "
            "match (t2:Track) where t1<>t2 and "
            "abs(t1.danceability - t2.danceability) < 0.025 and "
            "abs(t1.energy - t2.energy) < 0.025 and "
            "abs(t1.valence - t2.valence) < 0.025 "
            "return  distinct t2.id AS track_id, t2.danceability as dance, t2.energy AS energy, t2.valence as valence LIMIT 250")

            for record in records:
                track_id = record["track_id"] #proceso de guardado identico a rama1()
                parametros = [record["dance"], record["energy"], record["valence"]]
                tracks_data.append((track_id,parametros))

        return tracks_data #retorna la lista
            
    except Exception as e:
        logger.error(f"Error en la consulta: {e}")





def rama3():

    tracks_data = [] #crea una lista para guardar las canciones
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

             #este query selecciona las preferencias del usuario y de ahi busca canciones en las que:
             #danceability, energy y valence 
             #tengan una diferencia menor que 0.06 con sus preferencias
            records = session.run("match (u:User) where u.id = '1' "
            "match (t:Track ) where "
            "abs(t.danceability - u.danceability) < 0.06 and "
            "abs(t.energy - u.energy) < 0.06 and "
            "abs(t.valence - u.valence) < 0.06 "
            "return  distinct t.id AS track_id, t.danceability as dance, t.energy AS energy, t.valence as valence LIMIT 250")

            
            for record in records:
                track_id = record["track_id"]
                parametros = [record["dance"], record["energy"], record["valence"]]
                tracks_data.append((track_id,parametros))


        return tracks_data

    except Exception as e:
        logger.error(f"Error en la consulta: {e}")


        
parametros_medios()
preferencias()
list_rama1 = rama1() #guarda las 3 ramas en una variable
list_rama2 = rama2()
list_rama3 = rama3()

#combinacion de ramas
combinacion = {}
for track_id, parametros in list_rama1 + list_rama2 + list_rama3:
    if track_id not in combinacion: #utiliza un diccionario con los ids como llaves para eliminar los duplicados entre las 3 ramas
        combinacion[track_id] = parametros

arbol = [(track_id, parametros) for track_id, parametros in combinacion.items()] #regresa el diccionario ya sin duplicados a una lista


def resultados_de_ramas():  #imprime un resumen de la longitud de cada rama, y la longitud del arbol, mencionando cuantas canciones se duplicaban
    print("longitud de la rama 1: " + str(len(list_rama1)))
    print("longitud de la rama 2: " + str(len(list_rama2))) #Imprime longitudes de las ramas
    print("longitud de la rama 3: " + str(len(list_rama3)))

    duplicados = len(list_rama1) + len(list_rama2) + len(list_rama3) - len(arbol)

    print("longitud del arbol: " + str(len(arbol)) + ", se duplicaron " + str(duplicados) + " canciones")




