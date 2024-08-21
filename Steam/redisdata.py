import csv
import redis
import time

# Conectar a Redis
r = redis.StrictRedis(host='localhost', port=6379, db=1)

# Nombre del archivo CSV
csv_file = 'steam.csv'
c = 0

# Leer el archivo CSV y preparar los comandos de Redis
start_time = time.time()
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    pipeline = r.pipeline()
    for row in reader:
        if len(row) == 4:
            game_id, review, mood, help = row
            pipeline.hset(f"review:{c}", "id", game_id)
            pipeline.hset(f"review:{c}", "mood", mood)
            pipeline.hset(f"review:{c}", "review", review)
            pipeline.hset(f"review:{c}", "help", help)
            c += 1
            if c % 500 == 0:  # Ejecutar el pipeline cada 100 registros
                pipeline.execute()
    # Ejecutar cualquier comando restante en el pipeline
    pipeline.execute()

end_time = time.time()

# Calcular el tiempo total
total_time = end_time - start_time
print(f"Tiempo total de escritura en Redis: {total_time:.6f} segundos")