import sqlite3
import csv
import time

csv_file_path = 'steam.csv'

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()
c = 0
# Crear la tabla
cursor.execute('''
CREATE TABLE IF NOT EXISTS steam (
    c TEXT PRIMARY KEY,
    id TEXT,
    review TEXT,
    help TEXT,
    mood TEXT
)
''')

start_time = time.time()
with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
        mood = row['mood']
        id = row['id']
        review = row['review']
        help = row['help']
        # Insertar los datos en la base de datos
        cursor.execute('''
        INSERT INTO steam (
            c, id, review, help, mood
        ) VALUES (?, ?, ?, ?, ?)
        ''', (
            c, id, review, help, mood
        ))
        c += 1

# Confirmar los cambios
conn.commit()
end_time = time.time()
total_time = end_time - start_time
print(f"Base de datos SQLite creada y datos insertados exitosamente en {total_time:.6f} segundos.")

# Verificar los datos insertados
start_time = time.time()
cursor.execute('SELECT * FROM steam')
end_time = time.time()
total_time = end_time - start_time
print(f"Datos leídos exitosamente en {total_time:.6f} segundos.")


# Cerrar la conexión
conn.close()