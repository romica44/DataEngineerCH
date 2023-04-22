import requests
import json
import os
from dotenv import load_dotenv
import psycopg2
import itertools

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Hacer solicitud a la API y cargar los datos en un diccionario de Python
response = requests.get('https://api.spacexdata.com/v3/launches')
data = json.loads(response.text)

# Conectar con la base de datos de Redshift
conn = psycopg2.connect(
    host=os.environ['REDSHIFT_HOST'],
    port=os.environ['REDSHIFT_PORT'],
    user=os.environ['REDSHIFT_USER'],
    password=os.environ['REDSHIFT_PASSWORD'],
    database=os.environ['REDSHIFT_DATABASE']
)

#Crear cursor y tabla
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS spacex_launches (
        flight_number INT UNIQUE, 
        mission_name VARCHAR(2000),
        launch_date_utc TIMESTAMP,
        rocket_name VARCHAR(2000),
        launch_site_name VARCHAR(2000),
        details TEXT,
        PRIMARY KEY (flight_number)
    );
""")
conn.commit()

#Insertar los datos en la tabla
for launch in itertools.islice(data, 0, 50):
    cur.execute("""
        INSERT INTO spacex_launches (
            flight_number, mission_name, launch_date_utc, rocket_name, launch_site_name
        ) VALUES (
            %s, %s, %s, %s, %s
        );
    """, (
        launch['flight_number'],
        launch['mission_name'],
        launch['launch_date_utc'],
        launch['rocket']['rocket_name'],
        launch['launch_site']['site_name'],
    ))
conn.commit()

# Seleccionar todos los datos de la tabla
cur.execute("SELECT * FROM spacex_launches")
rows = cur.fetchall()

# Imprimir los datos en la consola
for row in rows:
    print(row)

cur.close()

# Cerrar la conexión con la base de datos
conn.close()

