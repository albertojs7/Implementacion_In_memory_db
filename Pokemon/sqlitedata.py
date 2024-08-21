import sqlite3
import csv
import time

csv_file_path = 'dataset.csv'

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()

# Crear la tabla
cursor.execute('''
CREATE TABLE IF NOT EXISTS pokemon (
    pokedex_number TEXT PRIMARY KEY,
    name TEXT,
    evolution TEXT,
    type TEXT,
    img TEXT,
    species TEXT,
    height TEXT,
    weight TEXT,
    abilities TEXT,
    local_number TEXT,
    ev_field TEXT,
    catch_rate TEXT,
    base_friendship TEXT,
    base_exp TEXT,
    growth_rate TEXT,
    egg_groups TEXT,
    gender TEXT,
    egg_cycles TEXT,
    hp TEXT,
    min_hp TEXT,
    max_hp TEXT,
    attack TEXT,
    min_attack TEXT,
    max_attack TEXT,
    defense TEXT,
    min_defense TEXT,
    max_defense TEXT,
    speed_attack TEXT,
    min_special_attack TEXT,
    max_special_attack TEXT,
    special_defense TEXT,
    min_special_defense TEXT,
    max_special_defense TEXT,
    speed TEXT,
    min_speed TEXT,
    max_speed TEXT
)
''')

start_time = time.time()
# Leer el archivo CSV desde el disco
with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
    reader = csv.DictReader(csv_file)
    
    # Preparar la inserción de datos
    for row in reader:
        name = row['pokemon_name']
        evolution = row['evolution_data']
        pokedex_number = row['pokemon_national_number']
        ptype = row['pokemon_type']
        abilities = row['pokemon_abilities']
        catch_rate = row['pokemon_catch_rate']
        hp = row['pokemon_hp']
        attack = row['pokemon_attack']
        defense = row['pokemon_defense']
        speed_attack = row['pokemon_speed_attack']
        img = row['pokemon_img']
        species = row['pokemon_species']
        height  = row['pokemon_height']
        weight = row['pokemon_weight']
        local_number = row['pokemon_local_number']
        ev_field = row['pokemon_ev_field']
        base_friendship = row['pokemon_base_friendship']
        base_exp = row['pokemon_base_exp']
        growth_rate = row['pokemon_growth_rate']
        egg_groups = row['pokemon_egg_groups']
        gender = row['pokemon_gender']
        egg_cycles = row['pokemon_egg_cycles']
        min_hp = row['pokemon_min_hp']
        max_hp = row['pokemon_max_hp']
        min_attack = row['pokemon_min_attack']
        max_attack = row['pokemon_max_attack']
        min_defense = row['pokemon_min_defense']
        max_defense = row['pokemon_max_defense']
        min_special_attack = row['pokemon_min_special_attack']
        max_special_attack = row['pokemon_max_special_attack']
        special_defense = row['pokemon_special_defense']
        min_special_defense = row['pokemon_min_special_defense']
        max_special_defense = row['pokemon_max_special_defense']
        speed = row['pokemon_speed']
        min_speed = row['pokemon_min_speed']
        max_speed = row['pokemon_max_speed']
        
        # Insertar los datos en la base de datos
        cursor.execute('''
        INSERT INTO pokemon (
            pokedex_number, name, evolution, type, img, species, height, weight, abilities, local_number, ev_field, catch_rate, base_friendship, base_exp, growth_rate, egg_groups, gender, egg_cycles, hp, min_hp, max_hp, attack, min_attack, max_attack, defense, min_defense, max_defense, speed_attack, min_special_attack, max_special_attack, special_defense, min_special_defense, max_special_defense, speed, min_speed, max_speed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            pokedex_number, name, evolution, ptype, img, species, height, weight, abilities, local_number, ev_field, catch_rate, base_friendship, base_exp, growth_rate, egg_groups, gender, egg_cycles, hp, min_hp, max_hp, attack, min_attack, max_attack, defense, min_defense, max_defense, speed_attack, min_special_attack, max_special_attack, special_defense, min_special_defense, max_special_defense, speed, min_speed, max_speed
        ))

# Confirmar los cambios
conn.commit()
end_time = time.time()
print(f"Base de datos SQLite creada y datos insertados exitosamente en {end_time - start_time} segundos.")

# Verificar los datos insertados
start_time = time.time()
cursor.execute('SELECT * FROM pokemon')
end_time = time.time()
print(f"Datos leídos exitosamente en {end_time - start_time} segundos.")


# Cerrar la conexión
conn.close()
