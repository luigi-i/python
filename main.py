from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging
import time
import random
from tabulate import tabulate
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de logging profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("recommendation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Clase para gestión de conexión Neo4j
class Neo4jConnection:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "DBMSPassword")
        self.driver = None
        self.connect()
    
    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            logger.info(f"Conexión establecida con {self.uri}")
        except Exception as e:
            logger.error(f"Error de conexión: {e}")
            self.driver = None
    
    def get_session(self):
        if self.driver:
            return self.driver.session()
        return None
    
    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Conexión cerrada")

# Configuración global
USER_ID = "1"  # ID de usuario por defecto

# Parámetros del algoritmo (ajustables)
R1_DANCE = 0.035
R1_ENERGY = 0.035
R1_VALENCE = 0.035

R2_DANCE = 0.025
R2_ENERGY = 0.025
R2_VALENCE = 0.025

R3_DANCE = 0.06
R3_ENERGY = 0.06
R3_VALENCE = 0.06

WEIGHT_1 = 0.4   # Historial reciente
WEIGHT_2 = 0.2   # Perfil histórico
WEIGHT_3 = 0.15  # Artista favorito
WEIGHT_4 = 0.1   # Género fav/no fav
WEIGHT_5 = 0.15  # En historial

# Inicializar conexión global
conn = Neo4jConnection()

# ===================================================
# Funciones optimizadas con manejo de errores
# ===================================================

def get_user_data(user_id):
    """Obtiene todos los datos del usuario en una sola consulta"""
    start_time = time.time()
    logger.info(f"Obteniendo datos para usuario {user_id}")
    
    try:
        session = conn.get_session()
        if not session:
            logger.error("No hay conexión a la base de datos")
            return None
        
        query = """
        MATCH (u:User {id: $user_id})
        OPTIONAL MATCH (u)-[:LISTENED_TO]->(t:Track)
        OPTIONAL MATCH (u)-[:LIKES_GENRE]->(fg:Genre)
        OPTIONAL MATCH (u)-[:DISLIKES_GENRE]->(dg:Genre)
        OPTIONAL MATCH (u)-[:FOLLOWS]->(a:Artist)
        RETURN 
            u.danceability AS dance_profile,
            u.energy AS energy_profile,
            u.valence AS valence_profile,
            COLLECT(DISTINCT t) AS historial_tracks,
            COLLECT(DISTINCT fg.name)[0] AS favorite_genre,
            COLLECT(DISTINCT dg.name)[0] AS disliked_genre,
            COLLECT(DISTINCT a.name)[0] AS favorite_artist,
            u.tempo_min AS tempo_min,
            u.tempo_max AS tempo_max
        """
        
        result = session.run(query, user_id=user_id)
        record = result.single()
        
        if not record:
            logger.warning(f"Usuario {user_id} no encontrado")
            return None
        
        # Procesar historial
        historial_tracks = record["historial_tracks"] or []
        historial_ids = []
        feature_values = {
            'acousticness': [],
            'danceability': [],
            'energy': [],
            'instrumentalness': [],
            'valence': [],
            'tempo': []
        }
        
        for track in historial_tracks:
            try:
                historial_ids.append(track.id)
                for feature in feature_values.keys():
                    if feature in track:
                        feature_values[feature].append(track[feature])
            except Exception as e:
                logger.error(f"Error procesando track: {e}")
        
        # Calcular promedios con manejo de listas vacías
        avg_hist = {}
        for feature, values in feature_values.items():
            avg_hist[feature] = np.mean(values) if values else 0.0
        
        # Verificar datos faltantes
        if not historial_ids:
            logger.warning(f"Usuario {user_id} no tiene historial de reproducción")
        
        # Construir objeto de retorno
        user_data = {
            "avg_hist": avg_hist,
            "preferences": {
                "favorite_genre": record["favorite_genre"] or "",
                "favorite_artist": record["favorite_artist"] or "",
                "disliked_genre": record["disliked_genre"] or "",
                "tempo_range": {
                    "min": record["tempo_min"] or 60,
                    "max": record["tempo_max"] or 180
                }
            },
            "profile": {
                "dance": record["dance_profile"] or 0.0,
                "energy": record["energy_profile"] or 0.0,
                "valence": record["valence_profile"] or 0.0
            },
            "historial": historial_ids[:30]  # Limitar a 30 elementos
        }
        
        logger.info(f"Datos obtenidos en {time.time() - start_time:.2f}s")
        return user_data
        
    except Exception as e:
        logger.error(f"Error en get_user_data: {e}")
        return None

def get_recommendation_candidates(user_id):
    """Obtiene candidatos de recomendación de las 3 ramas"""
    start_time = time.time()
    logger.info(f"Generando candidatos para usuario {user_id}")
    
    try:
        session = conn.get_session()
        if not session:
            logger.error("No hay conexión a la base de datos")
            return []
        
        # Diccionario para tracks únicos
        unique_tracks = {}
        
        # Rama 1: Basada en historial reciente
        query_rama1 = """
        MATCH (u:User)-[:LISTENED_TO]->(t1:Track)
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
        LIMIT 250
        """
        
        rama1 = session.run(query_rama1, 
                           user_id=user_id,
                           R1_DANCE=R1_DANCE,
                           R1_ENERGY=R1_ENERGY,
                           R1_VALENCE=R1_VALENCE)
        
        for record in rama1:
            track_id = record["track_id"]
            if track_id not in unique_tracks:
                unique_tracks[track_id] = {
                    "dance": record["dance"],
                    "energy": record["energy"],
                    "valence": record["valence"],
                    "genre": record["genre"],
                    "artist": record["artist"],
                    "track_name": record["track_name"]
                }
        
        # Rama 2: Basada en artista favorito
        query_rama2 = """
        MATCH (u:User)-[:FOLLOWS]->(a:Artist)-[:CREATED]-(t1:Track)
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
        LIMIT 250
        """
        
        rama2 = session.run(query_rama2, 
                           user_id=user_id,
                           R2_DANCE=R2_DANCE,
                           R2_ENERGY=R2_ENERGY,
                           R2_VALENCE=R2_VALENCE)
        
        for record in rama2:
            track_id = record["track_id"]
            if track_id not in unique_tracks:
                unique_tracks[track_id] = {
                    "dance": record["dance"],
                    "energy": record["energy"],
                    "valence": record["valence"],
                    "genre": record["genre"],
                    "artist": record["artist"],
                    "track_name": record["track_name"]
                }
        
        # Rama 3: Basada en perfil de usuario
        query_rama3 = """
        MATCH (u:User)
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
        LIMIT 250
        """
        
        rama3 = session.run(query_rama3, 
                           user_id=user_id,
                           R3_DANCE=R3_DANCE,
                           R3_ENERGY=R3_ENERGY,
                           R3_VALENCE=R3_VALENCE)
        
        for record in rama3:
            track_id = record["track_id"]
            if track_id not in unique_tracks:
                unique_tracks[track_id] = {
                    "dance": record["dance"],
                    "energy": record["energy"],
                    "valence": record["valence"],
                    "genre": record["genre"],
                    "artist": record["artist"],
                    "track_name": record["track_name"]
                }
        
        logger.info(f"{len(unique_tracks)} candidatos encontrados en {time.time() - start_time:.2f}s")
        return unique_tracks
        
    except Exception as e:
        logger.error(f"Error en get_recommendation_candidates: {e}")
        return {}

def calculate_scores(candidates, user_data):
    """Calcula puntuaciones con aleatoriedad y normalización"""
    start_time = time.time()
    logger.info("Calculando puntuaciones...")
    
    if not candidates:
        logger.warning("No hay candidatos para calificar")
        return []
    
    # Preparar vectores para similitud coseno
    avg_hist_vector = np.array([
        user_data["avg_hist"].get("danceability", 0),
        user_data["avg_hist"].get("energy", 0),
        user_data["avg_hist"].get("valence", 0)
    ]).reshape(1, -1)
    
    profile_vector = np.array([
        user_data["profile"].get("dance", 0),
        user_data["profile"].get("energy", 0),
        user_data["profile"].get("valence", 0)
    ]).reshape(1, -1)
    
    # Variables de preferencia
    prefs = user_data["preferences"]
    fav_artist = prefs["favorite_artist"]
    fav_genre = prefs["favorite_genre"]
    disliked_genre = prefs["disliked_genre"]
    history = user_data["historial"]
    
    scored_tracks = []
    
    for track_id, track_data in candidates.items():
        try:
            # Manejar valores nulos
            dance = track_data.get("dance", 0)
            energy = track_data.get("energy", 0)
            valence = track_data.get("valence", 0)
            genre = track_data.get("genre", "")
            artist = track_data.get("artist", "")
            track_name = track_data.get("track_name", f"Track_{track_id}")
            
            # Vector de características de la canción
            track_vector = np.array([dance, energy, valence]).reshape(1, -1)
            
            # Componentes de puntuación
            r1 = cosine_similarity(avg_hist_vector, track_vector)[0][0] if np.any(avg_hist_vector) else 0
            r2 = cosine_similarity(profile_vector, track_vector)[0][0] if np.any(profile_vector) else 0
            r3 = 1 if artist == fav_artist else 0
            r4 = 1 if genre == fav_genre else (-1 if genre == disliked_genre else 0)
            r5 = -1 if track_id in history else 0
            
            # Puntuación base
            base_score = (
                WEIGHT_1 * r1 + 
                WEIGHT_2 * r2 + 
                WEIGHT_3 * r3 + 
                WEIGHT_4 * r4 + 
                WEIGHT_5 * r5
            )
            
            # Añadir aleatoriedad (5% de variación)
            randomized_score = base_score * (1 + random.uniform(-0.05, 0.05))
            
            # Guardar resultados
            scored_tracks.append({
                "track_id": track_id,
                "track_name": track_name,
                "artist": artist,
                "genre": genre,
                "base_score": randomized_score,
                "components": [r1, r2, r3, r4, r5]
            })
            
        except Exception as e:
            logger.error(f"Error calificando {track_id}: {e}")
    
    # Normalización a escala 0-10
    if scored_tracks:
        min_score = min(track["base_score"] for track in scored_tracks)
        max_score = max(track["base_score"] for track in scored_tracks)
        
        # Evitar división por cero
        if max_score == min_score:
            max_score = min_score + 0.1
        
        for track in scored_tracks:
            track["normalized_score"] = 10 * (track["base_score"] - min_score) / (max_score - min_score)
    
    logger.info(f"Puntuaciones calculadas en {time.time() - start_time:.2f}s")
    return scored_tracks

def display_results(recommendations, user_id, top_n=30):
    """Muestra resultados en formato de tabla"""
    if not recommendations:
        print("\n⚠️ No se encontraron recomendaciones")
        return
    
    # Ordenar por puntuación normalizada
    sorted_recs = sorted(recommendations, key=lambda x: x["normalized_score"], reverse=True)[:top_n]
    
    # Preparar datos para tabla
    table_data = []
    for idx, rec in enumerate(sorted_recs, 1):
        table_data.append([
            idx,
            rec["track_name"],
            rec["artist"],
            rec["genre"],
            f"{rec['normalized_score']:.2f}/10",
            f"{rec['base_score']:.4f}"
        ])
    
    # Encabezados
    headers = ["#", "Canción", "Artista", "Género", "Puntuación", "Score Base"]
    
    # Mostrar tabla
    print("\n" + "=" * 80)
    print(f"🎵 RECOMENDACIONES PARA USUARIO {user_id}")
    print("=" * 80)
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    print("\n")
    
    # Exportar a CSV
    try:
        with open(f"recomendaciones_{user_id}.csv", "w") as f:
            f.write(tabulate(table_data, headers=headers, tablefmt="csv"))
        logger.info(f"Resultados exportados a recomendaciones_{user_id}.csv")
    except Exception as e:
        logger.error(f"Error exportando CSV: {e}")

# ===================================================
# Función principal
# ===================================================

def main():
    global USER_ID
    start_time = time.time()
    logger.info("🚀 Iniciando sistema de recomendación")
    
    # Cambiar USER_ID aquí si es necesario
    USER_ID = "1"
    
    # Obtener datos del usuario
    user_data = get_user_data(USER_ID)
    if not user_data:
        logger.error("No se puede continuar sin datos del usuario")
        return
    
    # Obtener candidatos
    candidates = get_recommendation_candidates(USER_ID)
    if not candidates:
        logger.error("No se encontraron candidatos para recomendación")
        return
    
    # Calificar canciones
    scored_recommendations = calculate_scores(candidates, user_data)
    
    # Mostrar resultados
    display_results(scored_recommendations, USER_ID)
    
    # Estadísticas finales
    total_time = time.time() - start_time
    logger.info(f"✅ Proceso completado en {total_time:.2f} segundos")

# Punto de entrada
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ejecución interrumpida por el usuario")
    except Exception as e:
        logger.exception(f"Error crítico: {e}")
    finally:
        conn.close()

# Cerrar conexión al final
if conn.driver:
    conn.close()
    logger.info("Conexión a Neo4j cerrada")