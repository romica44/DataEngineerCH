import requests
import json
import psycopg2
import itertools

# Hacer solicitud a la API y cargar los datos en un diccionario de Python
response = requests.get('https://api.spacexdata.com/v3/launches')
data = json.loads(response.text)

# Conectar con la base de datos de Redshift
conn = psycopg2.connect(
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    port = '5439',
    user = 'romica44_coderhouse',
    password = 'tJA6s0E8l2Cg',
    database = 'data-engineer-database'
)

#Crear cursor y tabla
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS spacex_launches (
        flight_number INT,
        mission_name VARCHAR(2000),
        launch_date_utc TIMESTAMP,
        rocket_name VARCHAR(2000),
        launch_site_name VARCHAR(2000),
        details TEXT
    );
""")
# conn.commit()

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

