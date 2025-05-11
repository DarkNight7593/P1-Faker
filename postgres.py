import os
import psycopg2
from faker import Faker
from random import choice, randint
from dotenv import load_dotenv

load_dotenv()
faker = Faker()

# ------------------ Conexión a PostgreSQL ------------------
try:
    print("📡 Conectando a PostgreSQL...")
    connection = psycopg2.connect(
        database=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PG_PORT')
    )
    print("✅ Conectado a PostgreSQL")
except psycopg2.Error as e:
    print(f"❌ Error al conectar a la base de datos: {e}")
    exit(1)

especialidades = [
    'Cardiología', 'Pediatría', 'Oftalmología', 'Neurología', 'Dermatología',
    'Ginecología/Obstetricia', 'Traumatología', 'Oncología', 'Urología', 'Psiquiatría',
    'Endocrinología', 'Reumatología', 'Hematología', 'Nefrología', 'Neumonología',
    'Nutriología', 'Odontología', 'Otorrinolaringología', 'Proctología', 'Radiología',
    'Toxicología', 'Anestesiología', 'Epidemiología', 'Geriatría', 'Medicina general',
    'Medicina interna', 'Psicología', 'Terapia física'
]

# ------------------ Crear tablas si no existen ------------------
with connection.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Doctor (
            dni VARCHAR(8) PRIMARY KEY,
            nombres VARCHAR(50),
            apellidos VARCHAR(50),
            especialidad VARCHAR(100),
            totalcitas INT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Disponibilidad (
            id SERIAL PRIMARY KEY,
            dia VARCHAR(15),
            hora TIME,
            dni_doctor VARCHAR(8),
            FOREIGN KEY (dni_doctor) REFERENCES Doctor(dni)
        );
    """)
    connection.commit()
    print("🛠️ Tablas creadas/verificadas.")

# ------------------ Generar doctores ------------------
def generar_doctores(num):
    doctores = []
    Faker.seed(0)
    for _ in range(num):
        dni = str(faker.unique.random_number(digits=8, fix_len=True))
        nombres = faker.first_name()
        apellidos = faker.last_name()
        especialidad = choice(especialidades)
        totalcitas = randint(0, 100)
        doctores.append((dni, nombres, apellidos, especialidad, totalcitas))
    return doctores

# ------------------ Generar disponibilidades ------------------
def generar_disponibilidades(doctores):
    disponibilidades = []
    dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
    for doctor in doctores:
        dni_doctor = doctor[0]
        for _ in range(randint(1, 5)):
            dia = choice(dias)
            hora = faker.time()
            disponibilidades.append((dia, hora, dni_doctor))
    return disponibilidades

# ------------------ Insertar datos ------------------
try:
    with connection.cursor() as cursor:
        print("📥 Insertando doctores...")
        doctores = generar_doctores(100)
        cursor.executemany("""
            INSERT INTO Doctor (dni, nombres, apellidos, especialidad, totalcitas)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dni) DO NOTHING;
        """, doctores)

        print("📥 Insertando disponibilidades...")
        disponibilidades = generar_disponibilidades(doctores)
        cursor.executemany("""
            INSERT INTO Disponibilidad (dia, hora, dni_doctor)
            VALUES (%s, %s, %s);
        """, disponibilidades)

        connection.commit()
        print("✅ Datos generados y almacenados exitosamente.")

except Exception as e:
    print(f"❌ Error al insertar datos: {e}")
finally:
    connection.close()
    print("🔒 Conexión cerrada.")
