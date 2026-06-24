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

# Base MySQL sur Windows
DB_HOST = "10.252.5.75"
DB_PORT = 3306
DB_USER = "saeuser"
DB_PASSWORD = "sae204"
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


def extraire_champ(pattern, payload, nom_champ):
    """
    Cherche un champ dans le message MQTT.
    Si le champ n'existe pas, retourne None au lieu de faire planter le programme.
    """
    resultat = re.search(pattern, payload)

    if resultat is None:
        print(f"[PARSE] Message ignoré, champ manquant : {nom_champ}")
        print(f"        Message reçu : {payload}")
        return None

    return resultat.group(1).strip()


def parse_message(payload):
    """
    Transforme le message MQTT en données exploitables.

    Message complet attendu :
    Id=A72E3F6B79BB,piece=sejour,date=18/06/2026,heure=12:13:14,temp=26,35

    Le script ignore les messages incomplets du style :
    Id=A72E3F6B79BB
    """

    try:
        # ID du capteur
        id_capteur = extraire_champ(r"(?:Id|ID|id)=([^,]+)", payload, "Id")
        if id_capteur is None:
            return None

        # Pièce
        piece = extraire_champ(r"piece=([^,]+)", payload, "piece")
        if piece is None:
            return None

        # Date
        date = extraire_champ(r"date=([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", payload, "date")
        if date is None:
            return None

        # Heure : accepte heure= ou time=
        heure = None

        resultat_heure = re.search(r"heure=([0-9]{1,2}:[0-9]{2}:[0-9]{2})", payload)
        resultat_time = re.search(r"time=([0-9]{1,2}:[0-9]{2}:[0-9]{2})", payload)

        if resultat_heure:
            heure = resultat_heure.group(1).strip()
        elif resultat_time:
            heure = resultat_time.group(1).strip()
        else:
            print("[PARSE] Message ignoré, champ manquant : heure ou time")
            print(f"        Message reçu : {payload}")
            return None

        # Température : accepte 26,35 ou 26.35 ou 26
        temp = extraire_champ(r"temp=(-?[0-9]+(?:[,.][0-9]+)?)", payload, "temp")
        if temp is None:
            return None

        # Conversion date JJ/MM/AAAA vers AAAA-MM-JJ pour MySQL
        jour, mois, annee = date.split("/")
        date_mesure = f"{annee}-{mois.zfill(2)}-{jour.zfill(2)} {heure}"

        # Conversion température 26,35 vers 26.35
        temperature = float(temp.replace(",", "."))

        return {
            "id_capteur": id_capteur,
            "piece": piece,
            "date_mesure": date_mesure,
            "temperature": temperature
        }

    except Exception as e:
        print(f"[PARSE] Erreur sur le message : {payload}")
        print(f"        Détail erreur : {e}")
        return None


def inserer_en_db(conn, data):
    """
    Insère un capteur s'il n'existe pas déjà,
    puis insère la mesure associée.
    """

    cursor = conn.cursor()

    # 1. Insertion du capteur
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

    # 2. Insertion de la mesure
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
        f"[DB] Inséré : "
        f"{data['id_capteur']} | "
        f"{data['piece']} | "
        f"{data['temperature']}°C | "
        f"{data['date_mesure']}"
    )


def vider_cache(conn):
    """
    Si la base était coupée, on réinsère les messages gardés en cache.
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
    Essaie d'insérer en base.
    Si la BDD est inaccessible, on met le message en cache.
    """
    global cache

    conn = connexion_db()

    if conn:
        try:
            vider_cache(conn)
            inserer_en_db(conn, data)
            conn.close()

        except Error as e:
            print(f"[DB] Erreur insertion : {e}")
            print("[CACHE] Message mis en cache")

            cache.append(data)

            if conn.is_connected():
                conn.close()

    else:
        cache.append(data)
        print(f"[CACHE] BDD indisponible, message mis en cache. Total : {len(cache)}")


# ============ CALLBACKS MQTT ============

def on_connect(client, userdata, flags, rc):
    """
    Fonction appelée quand le client se connecte au broker MQTT.
    """
    if rc == 0:
        print(f"[MQTT] Connecté au broker {BROKER}")

        for topic in TOPICS:
            client.subscribe(topic)
            print(f"[MQTT] Abonné au topic : {topic}")

    else:
        print(f"[MQTT] Erreur de connexion au broker. Code : {rc}")


def on_message(client, userdata, msg):
    """
    Fonction appelée à chaque message MQTT reçu.
    """
    payload = msg.payload.decode("utf-8").strip()

    print()
    print(f"[MQTT] Message reçu sur : {msg.topic}")
    print(f"[MQTT] Contenu : {payload}")

    data = parse_message(payload)

    if data:
        print(
            f"[PARSE] OK : "
            f"{data['id_capteur']} | "
            f"{data['piece']} | "
            f"{data['date_mesure']} | "
            f"{data['temperature']}°C"
        )

        traiter_message(data)
    else:
        print("[INFO] Message non inséré car incomplet ou incorrect.")


def on_disconnect(client, userdata, rc):
    """
    Fonction appelée quand le client se déconnecte du broker MQTT.
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
