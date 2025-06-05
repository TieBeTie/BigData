#!/usr/bin/env python3

from google.cloud import spanner
import csv
import time
from datetime import datetime
import os
import subprocess

PROJECT_ID = "hobod2025s-kozhagulovra"
INSTANCE_ID = "kozhagulovra-movielens-instance"
DATABASE_ID = "kozhagulovra-movielens-db"
CONFIG_NAME = "regional-europe-west1"

HDFS_MOVIES_PATH = "/data/movielens/movies.csv"
HDFS_RATINGS_PATH = "/data/movielens/ratings_20m.csv"

def download_from_hdfs(hdfs_path, local_path):
    try:
        print(f"📥 Скачиваем {hdfs_path} из HDFS...")
        cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"⚠️ Ошибка скачивания из HDFS: {result.stderr}")
            return False
            
        print(f"✅ Файл {local_path} скачан успешно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при работе с HDFS: {e}")
        return False

def load_movies_from_hdfs():
    """Загрузка данных о фильмах из HDFS"""
    local_movies_path = "/tmp/movies.csv"
    
    # Скачиваем из HDFS
    if not download_from_hdfs(HDFS_MOVIES_PATH, local_movies_path):
        print("❌ Не удалось скачать movies.csv из HDFS")
        return []
    
    movies_data = []
    try:
        print("📖 Читаем movies.csv...")
        with open(local_movies_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Пропускаем заголовок
            
            for row in csv_reader:
                if len(row) >= 3:
                    movie_id = int(row[0])
                    title = row[1]
                    # Парсим жанры (разделены символом |)
                    genres = row[2].split('|') if row[2] else []
                    
                    movies_data.append((movie_id, title, genres))
        
        print(f"✅ Загружено {len(movies_data)} фильмов из HDFS")
        return movies_data
        
    except Exception as e:
        print(f"❌ Ошибка при чтении movies.csv: {e}")
        return []
    finally:
        # Удаляем временный файл
        if os.path.exists(local_movies_path):
            os.remove(local_movies_path)

def load_ratings_from_hdfs(limit=10000):
    """Загрузка данных о рейтингах из HDFS"""
    local_ratings_path = "/tmp/ratings_20m.csv"
    
    # Скачиваем из HDFS
    if not download_from_hdfs(HDFS_RATINGS_PATH, local_ratings_path):
        print("❌ Не удалось скачать ratings_20m.csv из HDFS")
        return []
    
    ratings_data = []
    try:
        print(f"📖 Читаем ratings_20m.csv (первые {limit} записей)...")
        with open(local_ratings_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Пропускаем заголовок
            
            count = 0
            for row in csv_reader:
                if count >= limit:
                    break
                    
                if len(row) >= 4:
                    user_id = int(row[0])
                    movie_id = int(row[1]) 
                    rating = float(row[2])
                    # Конвертируем Unix timestamp в datetime
                    timestamp = datetime.fromtimestamp(int(row[3]))
                    
                    ratings_data.append((user_id, movie_id, rating, timestamp))
                    count += 1
        
        print(f"✅ Загружено {len(ratings_data)} рейтингов из HDFS")
        return ratings_data
        
    except Exception as e:
        print(f"❌ Ошибка при чтении ratings_20m.csv: {e}")
        return []
    finally:
        # Удаляем временный файл
        if os.path.exists(local_ratings_path):
            os.remove(local_ratings_path)

def create_spanner_instance():
    """Создание Spanner instance"""
    spanner_client = spanner.Client(project=PROJECT_ID)
    
    # Проверяем, существует ли instance
    try:
        instance = spanner_client.instance(INSTANCE_ID)
        if instance.exists():
            print(f"Instance {INSTANCE_ID} уже существует")
            return instance
    except:
        pass
    
    # Создаем новый instance
    print(f"Создаем Spanner instance: {INSTANCE_ID}")
    instance = spanner_client.instance(
        INSTANCE_ID,
        configuration_name=CONFIG_NAME,
        display_name="MovieLens Instance",
        node_count=1  # Минимальная конфигурация
    )
    
    operation = instance.create()
    print("Ожидание создания instance...")
    operation.result(timeout=300)  # 5 минут timeout
    print(f"Instance {INSTANCE_ID} создан!")
    return instance

def create_database_and_schema():
    """Создание базы данных и схемы"""
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    
    # Проверяем, существует ли база данных
    database = instance.database(DATABASE_ID)
    if database.exists():
        print(f"База данных {DATABASE_ID} уже существует")
        return database
    
    # DDL для создания таблиц (из README.md)
    ddl_statements = [
        """
        CREATE TABLE movies (
            movieId INT64 NOT NULL,
            title STRING(MAX),
            genres ARRAY<STRING(MAX)>
        ) PRIMARY KEY (movieId)
        """,
        """
        CREATE TABLE ratings (
            userId INT64 NOT NULL,
            movieId INT64 NOT NULL,
            rating FLOAT64,
            timestamp TIMESTAMP
        ) PRIMARY KEY (userId, movieId)
        """
    ]
    
    print(f"Создаем базу данных: {DATABASE_ID}")
    operation = database.create(ddl_statements=ddl_statements)
    print("Ожидание создания базы данных...")
    operation.result(timeout=300)
    print(f"База данных {DATABASE_ID} создана!")
    return database

def load_data_to_spanner():
    """Загрузка данных из HDFS в Spanner"""
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    
    print("🌊 Загружаем данные из HDFS в Spanner...")
    
    # 1. Загружаем фильмы из HDFS
    movies_data = load_movies_from_hdfs()
    if not movies_data:
        print("❌ Не удалось загрузить данные о фильмах")
        return False
    
    # 2. Загружаем рейтинги из HDFS (ограничиваем для демо)
    ratings_data = load_ratings_from_hdfs(limit=50000)  # 50K записей для демо
    if not ratings_data:
        print("❌ Не удалось загрузить данные о рейтингах")
        return False
    
    # 3. Фильтруем только те рейтинги, для которых есть фильмы
    movie_ids = {movie[0] for movie in movies_data}
    filtered_ratings = [r for r in ratings_data if r[1] in movie_ids]
    
    print(f"📊 Будем загружать:")
    print(f"  - Фильмов: {len(movies_data)}")
    print(f"  - Рейтингов: {len(filtered_ratings)}")
    
    # 4. Загружаем в Spanner транзакциями
    def insert_data(transaction):
        # Вставляем фильмы батчами
        batch_size = 1000
        print("📽️ Загружаем фильмы...")
        for i in range(0, len(movies_data), batch_size):
            batch = movies_data[i:i + batch_size]
            transaction.insert(
                table="movies",
                columns=("movieId", "title", "genres"),
                values=batch
            )
            print(f"  📦 Загружен батч фильмов {i//batch_size + 1}/{(len(movies_data)-1)//batch_size + 1}")
        
        # Вставляем рейтинги батчами
        print("⭐ Загружаем рейтинги...")
        for i in range(0, len(filtered_ratings), batch_size):
            batch = filtered_ratings[i:i + batch_size]
            transaction.insert(
                table="ratings", 
                columns=("userId", "movieId", "rating", "timestamp"),
                values=batch
            )
            print(f"  📦 Загружен батч рейтингов {i//batch_size + 1}/{(len(filtered_ratings)-1)//batch_size + 1}")
    
    try:
        database.run_in_transaction(insert_data)
        print(f"✅ Успешно загружено {len(movies_data)} фильмов и {len(filtered_ratings)} рейтингов")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных в Spanner: {e}")
        return False

def main():
    """Основная функция настройки"""
    print("=== Настройка Spanner для Task 1 (данные из HDFS) ===")
    
    try:
        # 1. Создаем instance
        create_spanner_instance()
        
        # 2. Создаем базу данных и схему
        create_database_and_schema()
        
        # 3. Загружаем данные из HDFS
        if load_data_to_spanner():
            print("\n✅ Настройка завершена успешно!")
            print(f"Instance: {INSTANCE_ID}")
            print(f"Database: {DATABASE_ID}")
            print("📊 Данные загружены из HDFS согласно README.md")
            print("🚀 Теперь можно запускать spannertask1_isolation_demo.py")
        else:
            print("\n❌ Ошибка при загрузке данных из HDFS")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    main() 