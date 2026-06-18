#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error

# ============ CONFIGURATION ============

BROKER = "broker.hivemq.com"
PORT = 1883

TOPICS = [
    "IUT/Colmar2026/SAE2.04/Maison1",
    "IUT/Colmar2026/SAE2.04/Maison2"
]

DB_HOST = "10.252.11.79"
DB_PORT = 3306
DB_USER = "toto"
DB_PASSWORD = "toto"
DB_NAME = "sae204"

# =======================================

cache = []


def connexion_db():
    """
    Essaie de se connecter à MySQL.
    Retourne la connexion si OK, sinon retourne None.
    """
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        print(f"[DB] Connecté à MySQL ({DB_HOST})")
        return conn

    except Error as e:
        print(f"[DB] Impossible de se connecter : {e}")
        return None


def parse_message(payload):
    """
    Transforme le message MQTT en données exploitables.

    Message attendu :
    Id=12A6B8AF6CD3,piece=sejour,date=15/06/2026,heure=12:13:14,temp=26,35
    """
    try:
        id_capteur = re.search(r"Id=([^,]+)", payload).group(1)
        piece = re.search(r"piece=([^,]+)", payload).group(1)
        date = re.search(r"date=([^,]+)", payload).group(1)
        heure = re.search(r"heure=([^,]+)", payload).group(1)
        temp = re.search(r"temp=([0-9]+(?:[,.][0-9]+)?)", payload).group(1)

        # Convertir date JJ/MM/AAAA vers AAAA-MM-JJ pour MySQL
        jour, mois, annee = date.split("/")
        date_mesure = f"{annee}-{mois}-{jour} {heure}"

        # Convertir 26,35 en 26.35
        temperature = float(temp.replace(",", "."))

        return {
            "id_capteur": id_capteur,
            "piece": piece,
            "date_mesure": date_mesure,
            "temperature": temperature
        }

    except Exception as e:
        print(f"[PARSE] Erreur sur le message '{payload}' : {e}")
        return None


def inserer_en_db(conn, data):
    """
    Insère un capteur s'il n'existe pas déjà,
    puis insère la mesure associée.
    """

    cursor = conn.cursor()

    # Table : capteur
    # Colonnes : id_capteur, nom, piece, emplacement
    cursor.execute("""
        INSERT IGNORE INTO capteur (id_capteur, nom, piece, emplacement)
        VALUES (%s, %s, %s, %s)
    """, (
        data["id_capteur"],
        f"Capteur_{data['id_capteur'][:6]}",
        data["piece"],
        data["piece"]
    ))

    # Table : mesure
    # Colonnes : id_capteur, date_mesure, temperature
    cursor.execute("""
        INSERT IGNORE INTO mesure (id_capteur, date_mesure, temperature)
        VALUES (%s, %s, %s)
    """, (
        data["id_capteur"],
        data["date_mesure"],
        data["temperature"]
    ))

    conn.commit()
    cursor.close()

    print(
        f"[DB] Inséré — "
        f"{data['id_capteur']} | "
        f"{data['piece']} | "
        f"{data['temperature']}°C | "
        f"{data['date_mesure']}"
    )


def vider_cache(conn):
    """
    Après une reconnexion DB, réinsère tous les messages stockés dans le cache.
    """
    global cache

    if not cache:
        return

    print(f"[CACHE] {len(cache)} message(s) en attente, réinsertion...")

    for data in cache[:]:
        try:
            inserer_en_db(conn, data)
            cache.remove(data)

        except Error as e:
            print(f"[CACHE] Échec réinsertion : {e}")
            break

    if not cache:
        print("[CACHE] Cache vidé")
    else:
        print(f"[CACHE] Il reste {len(cache)} message(s) dans le cache")


def traiter_message(data):
    """
    Essaie d'insérer en DB.
    Si la DB est indisponible, met le message en cache.
    """
    global cache

    conn = connexion_db()

    if conn:
        try:
            vider_cache(conn)
            inserer_en_db(conn, data)
            conn.close()

        except Error as e:
            print(f"[DB] Erreur insertion : {e} → mise en cache")
            cache.append(data)

            if conn.is_connected():
                conn.close()

    else:
        cache.append(data)
        print(f"[CACHE] DB indisponible → message mis en cache. Total : {len(cache)}")


# ============ CALLBACKS MQTT ============

def on_connect(client, userdata, flags, rc):
    """
    Appelé quand le client se connecte au broker MQTT.
    """
    if rc == 0:
        print(f"[MQTT] Connecté au broker {BROKER}")

        for topic in TOPICS:
            client.subscribe(topic)
            print(f"[MQTT] Abonné au topic : {topic}")

    else:
        print(f"[MQTT] Erreur de connexion au broker MQTT. Code : {rc}")


def on_message(client, userdata, msg):
    """
    Appelé à chaque message MQTT reçu.
    """
    payload = msg.payload.decode("utf-8").strip()

    print()
    print(f"[MQTT] Message reçu sur : {msg.topic}")
    print(f"[MQTT] Contenu : {payload}")

    data = parse_message(payload)

    if data:
        traiter_message(data)


def on_disconnect(client, userdata, rc):
    """
    Appelé quand le client se déconnecte du broker.
    """
    print(f"[MQTT] Déconnecté du broker. Code : {rc}")


# ============ PROGRAMME PRINCIPAL ============

if __name__ == "__main__":
    print("=" * 60)
    print(" SAE 2.04 - Collecte MQTT vers MySQL")
    print(f" Broker MQTT : {BROKER}:{PORT}")
    print(f" MySQL       : {DB_HOST}:{DB_PORT} / {DB_NAME}")
    print("=" * 60)

    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print("[INFO] Connexion au broker MQTT...")
    client.connect(BROKER, PORT, keepalive=60)

    print("[INFO] En écoute des messages MQTT... CTRL+C pour arrêter")
    client.loop_forever()