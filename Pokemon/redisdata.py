import csv
import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)

csv_file = 'dataset.csv'

start_time = time.time()
with open(csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    pipeline = r.pipeline()
    for row in reader:
        pokedex_number = row['pokemon_national_number']
        name = row['pokemon_name']
        evolution = row['evolution_data']
        ptype = row['pokemon_type']  
        pability = row['pokemon_abilities']
        pokemon_catch_rate = row['pokemon_catch_rate']
        pokemon_hp = row['pokemon_hp']
        pokemon_attack = row['pokemon_attack']
        pokemon_defense = row['pokemon_defense']
        pokemon_speed_attack = row['pokemon_speed_attack']
        pokemon_img = row['pokemon_img']
        pokemon_species = row['pokemon_species']
        pokemon_height  = row['pokemon_height']
        pokemon_weight = row['pokemon_weight']
        pokemon_abilities = row['pokemon_abilities']
        pokemon_local_number = row['pokemon_local_number']
        pokemon_ev_field = row['pokemon_ev_field']
        pokemon_base_friendship = row['pokemon_base_friendship']
        pokemon_base_exp = row['pokemon_base_exp']
        pokemon_growth_rate = row['pokemon_growth_rate']
        pokemon_egg_groups = row['pokemon_egg_groups']
        pokemon_gender = row['pokemon_gender']
        pokemon_egg_cycles = row['pokemon_egg_cycles']
        pokemon_min_hp = row['pokemon_min_hp']
        pokemon_max_hp = row['pokemon_max_hp']
        pokemon_min_attack = row['pokemon_min_attack']
        pokemon_max_attack = row['pokemon_max_attack']
        pokemon_min_defense = row['pokemon_min_defense']
        pokemon_max_defense = row['pokemon_max_defense']
        pokemon_min_special_attack = row['pokemon_min_special_attack']
        pokemon_max_special_attack = row['pokemon_max_special_attack']
        pokemon_special_defense = row['pokemon_special_defense']
        pokemon_min_special_defense = row['pokemon_min_special_defense']
        pokemon_max_special_defense = row['pokemon_max_special_defense']
        pokemon_speed = row['pokemon_speed']
        pokemon_min_speed = row['pokemon_min_speed']
        pokemon_max_speed = row['pokemon_max_speed']
        pipeline.execute()
        r.hset(f"pokemon:{pokedex_number}", "name", name)
        r.hset(f"pokemon:{pokedex_number}", "evolution", evolution)
        r.hset(f"pokemon:{pokedex_number}", "pokedex_number", pokedex_number)
        r.hset(f"pokemon:{pokedex_number}", "ptype", ptype)
        r.hset(f"pokemon:{pokedex_number}", "pability", pability)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_catch_rate", pokemon_catch_rate)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_hp", pokemon_hp)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_attack", pokemon_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_defense", pokemon_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_speed_attack", pokemon_speed_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_img", pokemon_img)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_species", pokemon_species)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_height", pokemon_height)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_weight", pokemon_weight)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_abilities", pokemon_abilities)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_local_number", pokemon_local_number)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_ev_field", pokemon_ev_field)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_base_friendship", pokemon_base_friendship)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_base_exp", pokemon_base_exp)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_growth_rate", pokemon_growth_rate)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_egg_groups", pokemon_egg_groups)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_gender", pokemon_gender)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_egg_cycles", pokemon_egg_cycles)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_hp", pokemon_min_hp)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_hp", pokemon_max_hp)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_attack", pokemon_min_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_attack", pokemon_max_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_defense", pokemon_min_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_defense", pokemon_max_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_special_attack", pokemon_min_special_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_special_attack", pokemon_max_special_attack)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_special_defense", pokemon_special_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_special_defense", pokemon_min_special_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_special_defense", pokemon_max_special_defense)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_speed", pokemon_speed)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_min_speed", pokemon_min_speed)
        r.hset(f"pokemon:{pokedex_number}", "pokemon_max_speed", pokemon_max_speed)
        pipeline.execute()
end_time = time.time()

# Calcular el tiempo total
total_time = end_time - start_time
print(f"Tiempo total de escritura en Redis: {total_time:.6f} segundos")

start_time = time.time()
keys = r.keys("pokemon:*")
for key in keys:
    pokemon_data = r.hgetall(key)
end_time = time.time()

total_time = end_time - start_time
print(f"Tiempo total de lectura en Redis: {total_time:.6f} segundos")

