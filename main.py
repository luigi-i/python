from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#ATRIBUTOS GLOBALES

USER_ID = "1"

#los 3 user id actuales tienen perfiles basados en:
#1. pop
#2. rap
#3. rock

#PARAMETROS DE CALIFICACION RAMA: DANCE, ENERGY, VALENCE

R1_DANCE = 0.015
R1_ENERGY = 0.015
R1_VALENCE = 0.015

R2_DANCE = 0.02
R2_ENERGY = 0.02
R2_VALENCE = 0.02

R3_DANCE = 0.015
R3_ENERGY = 0.015
R3_VALENCE = 0.015

#PARAMETROS DE PESO W

WEIGHT_1 = 0.5 # historial reciente
WEIGHT_2 = 0.3 #valores del perfil
WEIGHT_3 = 0.2 #artista fav
WEIGHT_4 = 0.35 #genero fav/no fav
WEIGHT_5 = 0.15 #esta en el historial?


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


def avg_historial():
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


def perfil():
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

            #obtener las 30 ultimas canciones del historial
            records = session.run("match (u:User) where u.id = '1' return u.danceability as dance, u.energy as energy, u.valence as valence ")

            record = records.single()
            output = {
                "dance": record["dance"],
                "energy": record["energy"],
                "valence": record["valence"]
            }

            return output

            
    except Exception as e:
        logger.error(f"Error en la consulta: {e}")

def historial():
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

            historial = []

            #obtener las 30 ultimas canciones del historial
            records = session.run("match (u:User)-[:LISTENED_TO]->(t:Track) where u.id = '1' limit 30 return t.id as track_id")

            for record in records:
                historial.append(record["track_id"])

        return historial

            
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
            query = """MATCH (u:User)-[:LISTENED_TO]->(t1:Track)
            WHERE u.id = $user_id
            WITH t1 LIMIT 20
            MATCH (t2:Track)-[:IN_PLAYLIST]->(p:Playlist)-[:HAS_GENRE]->(g:Genre),
                  (a:Artist)-[:CREATED]->(t2)
            WHERE t1 <> t2 AND
                  abs(t1.danceability - t2.danceability) < $R1_DANCE AND
                  abs(t1.energy - t2.energy) < $R1_ENERGY AND
                  abs(t1.valence - t2.valence) < $R1_VALENCE
            RETURN DISTINCT 
                  t2.id AS track_id, 
                  t2.danceability AS dance, 
                  t2.energy AS energy, 
                  t2.valence AS valence, 
                  t2.name AS track_name,
                  g.name AS genre, 
                  a.name AS artist 
            LIMIT 500"""

            records = session.run(query, user_id = USER_ID, #ejecuta el query con las variables especificadas al principio
                                  R1_ENERGY = R1_ENERGY, R1_DANCE = R1_DANCE, R1_VALENCE = R1_VALENCE)

            
            for record in records:
                track_id = record["track_id"] #guarda el id de la cancion 
                parametros = [record["dance"], record["energy"], record["valence"], record["genre"], record["artist"], record["track_name"]] #guarda los parametros que se utilizan en una lista
                tracks_data.append((track_id, parametros)) #guarda en parametros (lista):dance, energy,valence, genre, artist, track_name
            

        return tracks_data #retorna tracks data que contiene todos los registros de track_id y parametros

    except Exception as e:
        logger.error(f"Error en la consulta: {e}")

def rama2():

    tracks_data = [] #crea una lista para guardar las canciones
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

            #este query parte de las canciones del artista marcado como favorito y busca otras con una diferencia en:
            #danceability, energy y valence 
            #en las que tengan menos de un rango especifico de diferencia


            query = """MATCH (u:User)-[:FOLLOWS]->(a:Artist)-[:CREATED]-(t1:Track)
            WHERE u.id = $user_id
            WITH t1 LIMIT 20
            MATCH (t2:Track)-[:IN_PLAYLIST]->(p:Playlist)-[:HAS_GENRE]->(g:Genre),
                  (a:Artist)-[:CREATED]->(t2)
            WHERE t1 <> t2 AND
                  abs(t1.danceability - t2.danceability) < $R2_DANCE AND
                  abs(t1.energy - t2.energy) < $R2_ENERGY AND
                  abs(t1.valence - t2.valence) < $R2_VALENCE
            RETURN DISTINCT 
                  t2.id AS track_id,
                  t2.danceability AS dance,
                  t2.energy AS energy,
                  t2.valence AS valence,
                  t2.name AS track_name,
                  g.name AS genre,
                  a.name AS artist
            LIMIT 500"""

            records = session.run(query, user_id = USER_ID, #ejecuta el query con las variables especificadas al principio
                                  R2_ENERGY = R2_ENERGY, R2_DANCE = R2_DANCE, R2_VALENCE = R2_VALENCE)

            for record in records:
                track_id = record["track_id"] #guarda el id de la cancion 
                parametros = [record["dance"], record["energy"], record["valence"], record["genre"], record["artist"], record["track_name"]]
                tracks_data.append((track_id,parametros))#guarda en parametros (lista):dance, energy,valence, genre, artist, track_name

        return tracks_data #retorna tracks data que contiene todos los registros de track_id y parametros
            
    except Exception as e:
        logger.error(f"Error en la consulta: {e}")


def rama3():

    tracks_data = [] #crea una lista para guardar las canciones
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        with driver.session() as session:

             #este query selecciona las preferencias del usuario y de ahi busca canciones en las que:
             #danceability, energy y valence 
             #tengan una diferencia menor que un rango especificado con sus preferencias


            query = """MATCH (u:User)
            WHERE u.id = $user_id
            MATCH (t:Track)-[:IN_PLAYLIST]->(p:Playlist)-[:HAS_GENRE]->(g:Genre),
                  (a:Artist)-[:CREATED]->(t)
            WHERE
                  abs(t.danceability - u.danceability) < $R3_DANCE AND
                  abs(t.energy - u.energy) < $R3_ENERGY AND
                  abs(t.valence - u.valence) < $R3_VALENCE
            RETURN DISTINCT
                  t.id AS track_id,
                  t.danceability AS dance,
                  t.energy AS energy,
                  t.valence AS valence,
                  t.name AS track_name,
                  g.name AS genre,
                  a.name AS artist
            LIMIT 500"""
             
            records = session.run(query, user_id = USER_ID, #ejecuta el query con las variables especificadas al principio
                                  R3_ENERGY = R3_ENERGY, R3_DANCE = R3_DANCE, R3_VALENCE = R3_VALENCE )

            
            for record in records: 
                track_id = record["track_id"]#guarda el id de la cancion 
                parametros = [record["dance"], record["energy"], record["valence"], record["genre"], record["artist"], record["track_name"]] 
                tracks_data.append((track_id,parametros))#guarda en parametros (lista):dance, energy,valence, genre, artist, track_name


        return tracks_data#retorna tracks data que contiene todos los registros de track_id y parametros

    except Exception as e:
        logger.error(f"Error en la consulta: {e}")


        
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

def scoreSort():#algoritmo de calificacion de canciones

    ######comparacion vectorial########
    
    #extrae los valores del perfil e historial a un diccionario
    avg_hist = avg_historial()
    perf = perfil()
    puntuacion = preferencias()
    hist = historial()

    score_list = {}



    #valores del historial a vector
    avg_hist_val =np.array([avg_hist["danceability"], avg_hist["energy"], avg_hist["valence"]]).reshape(1, -1)

    #valores del perfil a vector
    perf_val = np.array([perf["dance"], perf["energy"], perf["valence"]]).reshape(1, -1)

    #para cada cancion del arbol
    for track_id, atributos in arbol:

        r1 = r2 = r3 = r4 = r5 = 0 #reinicia las puntuaciones a 0
        repeated = False #reinicia el: esta en el historial?

        vector = np.array(atributos[:3]).reshape(1,-1) #guarda en un vector dance, valence y energy de la cancion

        r1 = cosine_similarity(avg_hist_val, vector)[0][0] #comparacion vectorial coseno con el historial reciente y del perfil
        r2 = cosine_similarity(perf_val, vector)[0][0]

        if puntuacion["favorite_artist"] == atributos[4]: #suma puntos si es el artista favorito
            r3 = 1
        else:
            r3 = 0

        if puntuacion["favorite_genre"] == atributos[3]: #suma si es del genero favorito
            r4 = 1
        elif puntuacion["disliked_genres"] == atributos[3]:#resta si es del genero que no le gusta
            r4 = -1
        else: r4 = 0

        for track in hist:
            if track == track_id:  #true si la cancion esta en el historial
                repeated = True

        if repeated: r5 = -1

        #ponderacion

        score = (WEIGHT_1 * r1) + (WEIGHT_2 * r2) + (WEIGHT_3 * r3) + (WEIGHT_4 * r4) + (WEIGHT_5 * r5) 

        score_list[track_id] ={ #guarda la nota y el nombre de la cancion en un diccionario
            "track_name": atributos[5],
            "artist": atributos[4],
            "score": score
        }

    sorted_list = sorted(score_list.items(), key=lambda x: x[1]["score"], reverse= True) #metodo de sorteo extraido de la pagina de python

    top_songs = sorted_list[:30] #toma las 30 mejores notas

    return top_songs

top_songs = scoreSort()

for track_id, data in top_songs: #imprime los resultados
    print(f"Nombre: {data['track_name']}" )
    print(f"Score: {data['score']:.4f}")
    print(f"Artista: {data['artist']}")
    print("---------------------------")

