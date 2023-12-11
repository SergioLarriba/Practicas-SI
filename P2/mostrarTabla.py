from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

#Conectarse a la base de datos con url
DATABASE_URL = "postgresql://alumnodb:1234@localhost/si1"

#Crear engine SQLAlchemy
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

#Definir datos para la consulta
year1 = 2022
year2 = 2023

#Llamo a la consulta
sql_command = text("SELECT * FROM getTopSales(2022,2023)")
result = session.execute(sql_command, {'year1': year1, 'year2': year2}).fetchall()

#Imprimimos resultados
for fila in result:
    print(f"Year: {fila[0]}, Title: {fila[1].strip()}, Sales: {fila[2]}")


# Cierro la sesión
session.close()



