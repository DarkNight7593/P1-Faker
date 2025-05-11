import os
import sys
import random
from dotenv import load_dotenv
from faker import Faker
import psycopg2
import mysql.connector
from pymongo import MongoClient

# Cargar variables de entorno desde el archivo .env
load_dotenv()

faker = Faker()

# ------------------ Conexión a MongoDB ------------------
try:
    # Construir la URL de conexión incluyendo autenticación
    mongo_uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('HOST')}:{os.getenv('MONGO_PORT')}/"
    
    # Conectar a MongoDB usando la URI construida
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client['hospital']
    collection = db['Pacientes']
    dnis_pacientes = [pac['_id'] for pac in collection.find({}, {'_id': 1})]
    print(f"✅ Conectado a MongoDB — {len(dnis_pacientes)} pacientes cargados.")
except Exception as e:
    print(f"❌ Error al conectar a MongoDB: {e}")
    sys.exit(1)

# ------------------ Conexión a PostgreSQL ------------------
try:
    pg_conn = psycopg2.connect(
        database=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PG_PORT')
    )
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT dni, especialidad FROM doctores")
    doctores = pg_cursor.fetchall()
    print(f"✅ Conectado a PostgreSQL — {len(doctores)} doctores cargados.")
except Exception as e:
    print(f"❌ Error al conectar a PostgreSQL: {e}")
    sys.exit(1)

# ------------------ Conexión a MySQL ------------------
try:
    mysql_conn = mysql.connector.connect(
        host=os.getenv('HOST'),
        port=os.getenv('MYSQL_PORT'),  # Leer el puerto desde el .env
        database=os.getenv('MYSQL_DB'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD')
    )

    if mysql_conn.is_connected():
        print("✅ Conectado a MySQL")
        mysql_cursor = mysql_conn.cursor()

        # Crear tabla historias_clinicas
        mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS historias_clinicas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            dni VARCHAR(10) NOT NULL,
            fecha_creacion_historia DATETIME NOT NULL
        )
        """)

        # Crear tabla cita con dia_consulta como VARCHAR
        mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS cita (
            id INT AUTO_INCREMENT PRIMARY KEY,
            dni_doctor VARCHAR(10) NOT NULL,
            especialidad VARCHAR(50),
            dia_consulta VARCHAR(15) NOT NULL,
            hora_consulta TIME NOT NULL,
            dni VARCHAR(10) NOT NULL,
            diagnostico TEXT,
            tratamiento TEXT
        )
        """)

        dias_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']

        n_historias = 50

        for _ in range(n_historias):
            dni_paciente = random.choice(dnis_pacientes)
            fecha_historia = faker.date_time_this_year(before_now=True)

            # Insertar historia clínica
            mysql_cursor.execute("""
                INSERT INTO historias_clinicas (dni, fecha_creacion_historia)
                VALUES (%s, %s)
            """, (dni_paciente, fecha_historia))

            # Insertar entre 1 y 5 citas
            for _ in range(random.randint(1, 5)):
                dni_doctor, especialidad = random.choice(doctores)
                dia_consulta = random.choice(dias_semana)
                hora_consulta = faker.time(pattern="%H:%M:%S")
                diagnostico = faker.text(max_nb_chars=100)
                tratamiento = faker.text(max_nb_chars=100)

                mysql_cursor.execute("""
                    INSERT INTO cita (dni_doctor, especialidad, dia_consulta, hora_consulta, dni, diagnostico, tratamiento)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (dni_doctor, especialidad, dia_consulta, hora_consulta, dni_paciente, diagnostico, tratamiento))

                print(f"📝 Cita insertada: Paciente {dni_paciente} - Doctor {dni_doctor} ({dia_consulta})")

        mysql_conn.commit()
        print("✅ Todos los datos se insertaron correctamente.")

except Exception as e:
    print(f"❌ Error al conectar o insertar en MySQL: {e}")

finally:
    if 'mysql_conn' in locals() and mysql_conn.is_connected():
        mysql_cursor.close()
        mysql_conn.close()
        print("🔒 Conexión a MySQL cerrada.")
