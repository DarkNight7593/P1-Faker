import sys
import os
from pymongo import MongoClient
from faker import Faker
from random import choice, randint
from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

fake = Faker()

# Modelos Pydantic
class SeguroModel(BaseModel):
    tipo_seguro: str
    vencimiento: datetime

class PacienteModel(BaseModel):
    dni: str = Field(..., alias="_id")
    nombres: str
    apellidos: str
    fecha_nacimiento: date
    seguro: Optional[SeguroModel] = None

    class Config:
        populate_by_name = True

# Generador de DNI únicos (con chequeo en la base de datos)
def generar_dni_existente(collection) -> str:
    while True:
        dni = str(randint(10000000, 99999999))
        if not collection.find_one({"_id": dni}):
            return dni

# Conexión a MongoDB
try:
    # Leer los datos del archivo .env
    mongo_uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('HOST')}:{os.getenv('MONGO_PORT')}/"
    client = MongoClient(mongo_uri)
    client.server_info()  # Verificar si la conexión es exitosa
    print("✅ Conectado a MongoDB")
except Exception as e:
    print("❌ Error al conectar a MongoDB:", e)
    sys.exit(1)

# Cambia estos nombres según tu base real
db = client.hospital  # ← Cambia aquí tu base
collection = db.pacientes  # ← Cambia aquí tu colección

# Configuración
cantidad_pacientes = 50
limpiar_coleccion = True  # ← Cambia a False si no quieres borrar datos anteriores

seguros = ["Salud", "Vida", "Dental"]
data = []

if limpiar_coleccion:
    collection.delete_many({})
    print("🧹 Colección limpiada antes de insertar nuevos datos.")

for _ in range(cantidad_pacientes):
    tiene_seguro = choice([True, False])
    fecha_nacimiento = fake.date_of_birth(minimum_age=1, maximum_age=99)

    paciente = PacienteModel(
        dni=generar_dni_existente(collection),
        nombres=fake.first_name(),
        apellidos=fake.last_name(),
        fecha_nacimiento=fecha_nacimiento
    )

    if tiene_seguro:
        tipo_seguro = choice(seguros)
        vencimiento = fake.date_between(start_date='today', end_date='+2y')
        paciente.seguro = SeguroModel(tipo_seguro=tipo_seguro, vencimiento=vencimiento)

    paciente_dict = paciente.dict(by_alias=True)
    paciente_dict['fecha_nacimiento'] = datetime.combine(paciente_dict['fecha_nacimiento'], datetime.min.time())
    data.append(paciente_dict)

if data:
    result = collection.insert_many(data)
    print(f"✅ {len(result.inserted_ids)} pacientes insertados correctamente.")
else:
    print("⚠️ No se generaron pacientes para insertar.")

