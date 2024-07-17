import pymysql

def connect_to_database():
    try:
        # Configuración de la conexión a la base de datos
        connection = pymysql.connect(
            host='mysql-inteligenciadenegocios.alwaysdata.net',
            port=3306,
            user='367255_admin',
            password='fajikej627',
            database='inteligenciadenegocios_access',
            connect_timeout=6000  # Tiempo de espera en segundos
        )
        print("Connected to database")
        return connection
    except pymysql.MySQLError as e:
        print("Error connecting to database:", e)
        return None
