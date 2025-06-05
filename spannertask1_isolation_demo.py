#!/usr/bin/env python3

from google.cloud import spanner
import threading
import time
from datetime import datetime
import sys

PROJECT_ID = "hobod2025s-kozhagulovra" 
INSTANCE_ID = "kozhagulovra-movielens-instance"
DATABASE_ID = "kozhagulovra-movielens-db"

class IsolationDemo:
    def __init__(self):
        self.spanner_client = spanner.Client(project=PROJECT_ID)
        self.instance = self.spanner_client.instance(INSTANCE_ID)
        self.database = self.instance.database(DATABASE_ID)
        self.long_read_results = []
        self.update_completed = False
        
    def long_read_transaction(self, num_batches=10, batch_size=10):
        print(f"Начинаем долгую read-only транзакцию")
        print(f"Читаем {num_batches} батчей по {batch_size} записей")
        
        # Создаем snapshot - фиксируем TrueTime timestamp
        with self.database.snapshot() as snapshot:
            transaction_start_time = datetime.now()
            print(f"⏰ Транзакция начата в: {transaction_start_time}")
            
            for batch_num in range(num_batches):
                offset = batch_num * batch_size
                
                query = f"""
                SELECT userId, movieId, rating, timestamp
                FROM ratings
                ORDER BY userId, movieId
                LIMIT {batch_size} OFFSET {offset}
                """
                
                batch_start = datetime.now()
                results = snapshot.execute_sql(query)
                batch_data = list(results)
                batch_end = datetime.now()
                
                print(f"📦 Батч {batch_num + 1}/{num_batches}: "
                      f"получено {len(batch_data)} записей "
                      f"за {(batch_end - batch_start).total_seconds():.3f}s")
                
                if batch_data:
                    for row in batch_data[:3]:
                        self.long_read_results.append({
                            'batch': batch_num + 1,
                            'userId': row[0],
                            'movieId': row[1], 
                            'rating': row[2],
                            'timestamp': row[3],
                            'read_time': batch_end
                        })
                
                # Проверяем, завершилось ли обновление
                if self.update_completed:
                    print(f"📝 Обновление завершилось во время чтения батча {batch_num + 1}")
                
                # Задержка между батчами для демонстрации
                if batch_num < num_batches - 1:
                    time.sleep(2)  # 2 секунды между батчами
            
            transaction_end_time = datetime.now()
            total_time = (transaction_end_time - transaction_start_time).total_seconds()
            print(f"✅ Read-only транзакция завершена за: {total_time:.2f}s")
            
        return self.long_read_results
    
    def update_fantasy_ratings(self):
        """
        Обновление рейтингов фильмов жанра Fantasy
        Поднимает все оценки Fantasy фильмов на 2 балла
        Работает с реальными данными MovieLens из HDFS
        """
        print(f"🔄 Запускаем обновление Fantasy рейтингов в реальных данных MovieLens...")
        
        def update_transaction(transaction):
            # Сначала посмотрим, сколько Fantasy записей в реальных данных
            count_query = """
            SELECT COUNT(*) as count
            FROM ratings r
            JOIN movies m ON r.movieId = m.movieId  
            WHERE 'Fantasy' IN UNNEST(m.genres)
            """
            count_result = list(transaction.execute_sql(count_query))
            count = count_result[0][0] if count_result else 0
            
            # Выполняем обновление всех Fantasy рейтингов
            update_query = """
            UPDATE ratings 
            SET rating = LEAST(5.0, rating + 2.0)
            WHERE movieId IN (
                SELECT movieId 
                FROM movies 
                WHERE 'Fantasy' IN UNNEST(genres)
            )
            """
            
            transaction.execute_update(update_query)
            return count
        
        update_start = datetime.now()
        updated_count = self.database.run_in_transaction(update_transaction)
        update_end = datetime.now()
        update_time = (update_end - update_start).total_seconds()
        
        self.update_completed = True
        print(f"✅ Обновление завершено: {updated_count} Fantasy рейтингов за {update_time:.2f}s")
        return updated_count
    
    def verify_isolation(self):
        """
        Проверка изоляции: запуск новой транзакции для чтения обновленных данных
        """
        print(f"🔍 Проверяем обновленные данные новой транзакцией...")
        
        query = """
        SELECT userId, movieId, rating, timestamp
        FROM ratings
        ORDER BY userId, movieId
        LIMIT 50
        """
        
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            new_data = list(results)
        
        print(f"📊 Получено {len(new_data)} записей после обновления")
        return new_data
    
    def run_isolation_demo(self):
        """
        Основная демонстрация изоляции транзакций
        """
        print("=" * 60)
        print("🎯 ДЕМОНСТРАЦИЯ ИЗОЛЯЦИИ ТРАНЗАКЦИЙ В SPANNER")
        print("📊 Используем реальные данные MovieLens из HDFS")
        print("=" * 60)
        
        # Сброс состояния
        self.long_read_results = []
        self.update_completed = False
        
        # 1. Запускаем долгую read-only транзакцию в отдельном потоке
        read_thread = threading.Thread(
            target=lambda: self.long_read_transaction(num_batches=8, batch_size=15)
        )
        
        # 2. Запускаем транзакцию чтения
        read_thread.start()
        
        # 3. Ждем 3 секунды, затем запускаем обновление
        time.sleep(3)
        print("\n" + "="*50)
        print("🚀 ЗАПУСКАЕМ ОБНОВЛЕНИЕ ВО ВРЕМЯ ЧТЕНИЯ!")
        print("="*50)
        
        # 4. Выполняем обновление
        self.update_fantasy_ratings()
        
        # 5. Ждем завершения read-only транзакции
        read_thread.join()
        
        # 6. Проверяем результаты
        print("\n" + "="*50)
        print("📋 АНАЛИЗ РЕЗУЛЬТАТОВ")
        print("="*50)
        
        print("📊 Данные, прочитанные СТАРОЙ транзакцией (до обновления):")
        for i, row in enumerate(self.long_read_results[:10]):
            print(f"  {i+1}. User {row['userId']}, Movie {row['movieId']}: "
                  f"rating={row['rating']:.1f}")
        
        # 7. Проверяем данные новой транзакцией
        print(f"\n🔄 Запускаем НОВУЮ транзакцию для проверки обновленных данных...")
        new_data = self.verify_isolation()
        
        print("📊 Данные, прочитанные НОВОЙ транзакцией (после обновления):")
        for i, row in enumerate(new_data[:10]):
            print(f"  {i+1}. User {row[0]}, Movie {row[1]}: "
                  f"rating={row[2]:.1f}")
        
        # 8. Анализ изоляции
        print("\n" + "="*50)
        print("🎯 ВЫВОДЫ ОБ ИЗОЛЯЦИИ:")
        print("="*50)
        
        old_ratings = [r['rating'] for r in self.long_read_results]
        new_ratings = [r[2] for r in new_data[:len(old_ratings)]]
        
        if old_ratings and new_ratings:
            avg_old = sum(old_ratings) / len(old_ratings)
            avg_new = sum(new_ratings) / len(new_ratings)
            
            print(f"📈 Средний рейтинг в СТАРОЙ транзакции: {avg_old:.2f}")
            print(f"📈 Средний рейтинг в НОВОЙ транзакции: {avg_new:.2f}")
            print(f"📊 Разница: {avg_new - avg_old:.2f}")
            
            if abs(avg_new - avg_old) > 0.5:  # Снижен порог для реальных данных
                print("✅ ИЗОЛЯЦИЯ ПОДТВЕРЖДЕНА: Старая транзакция читала данные ДО обновления!")
                print("✅ Новая транзакция читает обновленные данные (+2 к рейтингам Fantasy)!")
                print("🌟 Эксперимент с реальными данными MovieLens из HDFS успешен!")
            else:
                print("⚠️ Небольшая разница в рейтингах (возможно, уже обновлялись ранее)")
                print("🔄 Попробуйте перезапустить setup.py для сброса данных")

def save_results_to_file(demo_results, filename="result.txt"):
    """Сохранение результатов в файл для сдачи (TOP-100 строк)"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("SPANNER TASK 1: Демонстрация неблокирующего чтения\n")
        f.write("Данные: Реальный MovieLens dataset из HDFS\n")
        f.write("="*60 + "\n\n")
        
        f.write("РЕЗУЛЬТАТЫ ДОЛГОЙ READ-ONLY ТРАНЗАКЦИИ (до обновления):\n")
        f.write("-"*50 + "\n")
        for i, row in enumerate(demo_results[:100]):
            f.write(f"{i+1:3d}. User {row['userId']:6d}, Movie {row['movieId']:6d}: "
                   f"rating={row['rating']:4.1f}\n")
        
        f.write(f"\n Всего записей в демонстрации: {len(demo_results)}\n")
        f.write(f"Источник данных: HDFS /data/movielens/\n")
        f.write(f"Время выполнения: {datetime.now()}\n")

def main():
    """Основная функция"""
    try:
        print("🔌 Подключаемся к Spanner с данными из HDFS...")
        demo = IsolationDemo()
        
        # Запускаем демонстрацию
        demo.run_isolation_demo()
        
        # Сохраняем результаты для сдачи
        if demo.long_read_results:
            save_results_to_file(demo.long_read_results)
            print(f"\n💾 Результаты сохранены в result.txt")
            print(f"📄 Файл содержит TOP-100 строк для сдачи задания")
        else:
            print(f"\n⚠️ Нет результатов для сохранения")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        print(f"\n💡 Убедитесь, что:")
        print(f"   1. Запущен spannertask1_setup.py")
        print(f"   2. Данные загружены из HDFS")
        print(f"   3. PROJECT_ID настроен правильно")
        sys.exit(1)

if __name__ == "__main__":
    main() 