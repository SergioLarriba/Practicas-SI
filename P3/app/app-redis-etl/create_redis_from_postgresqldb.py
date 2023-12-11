import random
import redis
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import Session

# Conexión a la base de datos PostgreSQL
"""
Url con:
    Usuario: alumnodb
    Contraseña: 1234
    Base de datos: si1
    host: localhost
"""
print("Conectando a la base de datos PostgreSQL...")
engine = create_engine('postgresql://alumnodb:1234@localhost:5432/si1')
metadata = MetaData()
metadata.bind = engine
customers_table = Table('customers', metadata, autoload_with=engine)

# Conexión a la base de datos Redis
print("Conectando a la base de datos Redis...")
redis_db = redis.Redis(host='localhost', port=6379, db=0)

# Crear una sesión
session = Session(engine)

def create_redis_db():
    print("Creando base de datos Redis...")
    # Obtener clientes de la base de datos PostgreSQL que son de España
    query = customers_table.select().where(customers_table.c.country == 'Spain')
    result = session.execute(query)
    customers = result.fetchall()
    
    # Almacenar clientes en la base de datos Redis
    for customer in customers:
        email = customer.email
        name = customer.firstname + " " + customer.lastname
        phone = customer.phone
        visits = random.randint(1, 99)
        
        hash_key = f"customers:{email}"
        redis_db.hset(hash_key, "name", name)
        redis_db.hset(hash_key, "phone", phone)
        redis_db.hset(hash_key, "visits", visits)
    
    print("Base de datos Redis creada!.")

def increment_by_email(email):
    hash_key = f"customers:{email}"
    redis_db.hincrby(hash_key, "visits", 1)

def customer_most_visits():
    customers = redis_db.keys("customers:*")
    max_visits = 0
    max_email = ""
    
    for customer in customers:
        visits = int(redis_db.hget(customer, "visits"))
        if visits > max_visits:
            max_visits = visits
            max_email = customer.decode("utf-8").split(":")[1]
    
    return max_email

def get_field_by_email(email):
    hash_key = f"customers:{email}"
    name = redis_db.hget(hash_key, "name").decode("utf-8")
    phone = redis_db.hget(hash_key, "phone").decode("utf-8")
    visits = int(redis_db.hget(hash_key, "visits"))
    
    return name, phone, visits

# Funcion para obtener las visitas dado un email
def get_visits_by_email(email):
    hash_key = f"customers:{email}"
    visits = int(redis_db.hget(hash_key, "visits"))
    return visits 

# Creacion de la base de datos Redis a partir de PostgreSQL
create_redis_db()

# CONSULTAS 

# Consulta 1: Incrementar en 1 las visitas de un cliente
email = "ballsy.cobra@jmail.com"
print(f"\nQuery 1: Incrementar en 1 las visitas de un cliente - Cliente: {email}") 
print(f"Visitas antes: {get_visits_by_email(email)}")
increment_by_email(email)
print(f"Visitas despues: {get_visits_by_email(email)}")

# Consulta 2: Obtener el email del usuario con mas visitas 
print(f"\nQuery 2: obtener el email con mas visitas: {customer_most_visits()}") 

# Consulta 3: Obtener el nombre, telefono y visitas de un cliente
print(f"\nQuery 3: Obtener el nombre, telefono y visitas de un cliente - Cliente: {email}")
name, phone, visits = get_field_by_email(email)
print(f"Nombre: {name} - Telefono: {phone} - Visitas: {visits}")


