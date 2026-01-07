import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime
import random
import os
import json


def create_metadata_file(generated_files, num_files, rows_per_file, total_duration, output_dir):
    """
    Создание файла с метаданными о сгенерированных данных
    """
    metadata = {
        'generation_date': datetime.now().isoformat(),
        'total_files': num_files,
        'rows_per_file': rows_per_file,
        'total_rows': num_files * rows_per_file,
        'total_duration_seconds': total_duration,
        'average_speed_rows_per_second': (num_files * rows_per_file) / total_duration if total_duration > 0 else 0,
        'output_directory': os.path.abspath(output_dir),
        'columns': [
            'user_id', 'first_name', 'last_name', 'email',
            'age', 'salary', 'department', 'hire_date',
            'city', 'is_active', 'score', 'last_login',
            'transaction_amount', 'product_category', 'order_date',
            'phone_number', 'postal_code', 'company', 'job_title',
            'credit_score', 'account_balance', 'last_purchase_date'
        ],
        'file_list': [
            {
                'filename': os.path.basename(filepath),
                'path': os.path.abspath(filepath),
                'size_bytes': os.path.getsize(filepath) if os.path.exists(filepath) else 0
            }
            for filepath in generated_files
        ]
    }

    metadata_file = os.path.join(output_dir, '_metadata.json')
    try:
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n📄 Метаданные сохранены в: {os.path.abspath(metadata_file)}")
    except Exception as e:
        print(f"\n⚠ Не удалось сохранить метаданные: {e}")


def generate_large_csv(filename, rows=10000, batch_size=2000, seed_offset=0, output_dir='data/input'):
    """
    Генерация CSV файла с синтетическими данными

    Parameters:
    -----------
    filename : str
        Имя файла для сохранения
    rows : int
        Количество строк в файле (по умолчанию 10,000)
    batch_size : int
        Размер батча для генерации (по умолчанию 2,000)
    seed_offset : int
        Смещение для seed, чтобы данные в разных файлах были разными
    output_dir : str
        Папка для сохранения файлов (по умолчанию 'data/input')
    """
    fake = Faker('ru_RU')  # Русские данные

    # Уникальные seed для каждого файла
    Faker.seed(42 + seed_offset)
    np.random.seed(42 + seed_offset)
    random.seed(42 + seed_offset)

    columns = [
        'user_id', 'first_name', 'last_name', 'email',
        'age', 'salary', 'department', 'hire_date',
        'city', 'is_active', 'score', 'last_login',
        'transaction_amount', 'product_category', 'order_date',
        'phone_number', 'postal_code', 'company', 'job_title',
        'credit_score', 'account_balance', 'last_purchase_date'
    ]

    # Создаем директорию если нет
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    print(f"Генерация файла '{filename}' с {rows:,} строками...")
    start_time = datetime.now()

    # Генерация данных по батчам для экономии памяти
    for batch_num in range(0, rows, batch_size):
        batch_rows = min(batch_size, rows - batch_num)
        batch_data = []

        for i in range(batch_rows):
            user_id = batch_num + i + 1
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            age = np.random.randint(18, 75)
            salary = round(np.random.normal(60000, 20000), 2)
            department = np.random.choice(['IT', 'Sales', 'HR', 'Marketing', 'Finance', 'Operations', 'Support', 'R&D'])
            hire_date = fake.date_between(start_date='-10y', end_date='today')
            city = fake.city()
            is_active = np.random.choice([True, False], p=[0.85, 0.15])
            score = round(np.random.uniform(0, 100), 2)
            last_login = fake.date_time_between(start_date='-60d', end_date='now')
            transaction_amount = round(np.random.exponential(150), 2)
            product_category = np.random.choice(
                ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Automotive', 'Health', 'Sports'])
            order_date = fake.date_between(start_date='-180d', end_date='today')
            phone_number = fake.phone_number()
            postal_code = fake.postcode()
            company = fake.company()
            job_title = fake.job()
            credit_score = np.random.randint(300, 850)
            account_balance = round(np.random.uniform(-5000, 50000), 2)
            last_purchase_date = fake.date_between(start_date='-365d',
                                                   end_date='today') if np.random.random() > 0.3 else None

            batch_data.append([
                user_id, first_name, last_name, email, age, salary,
                department, hire_date, city, is_active, score,
                last_login, transaction_amount, product_category, order_date,
                phone_number, postal_code, company, job_title,
                credit_score, account_balance, last_purchase_date
            ])

        # Создаем DataFrame для батча
        df_batch = pd.DataFrame(batch_data, columns=columns)

        # Записываем в файл (добавляем если не первый батч)
        if batch_num == 0:
            df_batch.to_csv(filepath, index=False)
        else:
            df_batch.to_csv(filepath, mode='a', header=False, index=False)

        # Показываем прогресс
        completed = batch_num + batch_rows
        progress = (completed / rows) * 100
        print(f"  Прогресс: {progress:.1f}% ({completed:,}/{rows:,} строк)")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"✓ Файл '{filename}' успешно создан!")
    print(f"  Время генерации: {duration:.2f} секунд")
    print(f"  Скорость: {rows / duration:.0f} строк/сек")
    print(f"  Полный путь: {os.path.abspath(filepath)}\n")
    return filepath


def generate_filenames(num_files=15):
    """
    Генерация осмысленных имен для файлов с номерами пачек

    Parameters:
    -----------
    num_files : int
        Количество файлов для генерации
    """
    current_date = datetime.now().strftime("%Y%m%d")
    current_time = datetime.now().strftime("%H%M%S")  # Добавлено: ТОЛЬКО время час-минута-секунда

    filenames = []
    for i in range(1, num_files + 1):
        # Обновленный формат: data_batch_001_20241218_143025.csv
        filename = f"data_batch_{i:03d}_{current_date}_{current_time}.csv"
        filenames.append(filename)

    return filenames


# Генерация 15 файлов по 10,000 строк
if __name__ == "__main__":
    files_to_generate = 15
    rows_per_file = 10000
    output_directory = 'data/input'  # Новая папка для сохранения

    print("=" * 70)
    print("🚀 ГЕНЕРАЦИЯ БОЛЬШИХ ФАЙЛОВ ДЛЯ ДАТА-ИНЖЕНЕРА")
    print("=" * 70)
    print(f"Количество файлов: {files_to_generate}")
    print(f"Строк в каждом файле: {rows_per_file:,}")
    print(f"Общее количество строк: {files_to_generate * rows_per_file:,}")
    print(f"Ориентировочный размер: ~{files_to_generate * rows_per_file * 300 / (1024 * 1024):.1f} MB")
    print(f"Папка назначения: {os.path.abspath(output_directory)}")
    print("=" * 70)

    # Создаем папку если не существует
    os.makedirs(output_directory, exist_ok=True)

    # Генерируем имена файлов
    filenames = generate_filenames(files_to_generate)

    total_start_time = datetime.now()
    generated_files = []

    for i, filename in enumerate(filenames, 1):
        print(f"\n[{i}/{files_to_generate}] Начало генерации: {filename}")

        # Увеличиваем seed_offset для более значительных различий между файлами
        seed_offset = i * 10000  # Большое смещение для больших различий

        filepath = generate_large_csv(
            filename=filename,
            rows=rows_per_file,
            batch_size=2000,  # Увеличили батч для скорости
            seed_offset=seed_offset,
            output_dir=output_directory  # Передаем папку для сохранения
        )

        generated_files.append(filepath)

        # Оценка оставшегося времени
        if i > 1:
            elapsed = (datetime.now() - total_start_time).total_seconds()
            avg_time_per_file = elapsed / i
            remaining_files = files_to_generate - i
            estimated_remaining = avg_time_per_file * remaining_files

            print(f"   Осталось: {remaining_files} файлов (~{estimated_remaining:.0f} сек)")

    total_end_time = datetime.now()
    total_duration = (total_end_time - total_start_time).total_seconds()

    print("\n" + "=" * 70)
    print("✅ ГЕНЕРАЦИЯ ЗАВЕРШЕНА!")
    print("=" * 70)

    # Собираем статистику
    total_size = 0
    for filepath in generated_files:
        if os.path.exists(filepath):
            total_size += os.path.getsize(filepath)

    total_rows = files_to_generate * rows_per_file

    print(f"📊 СТАТИСТИКА:")
    print(f"   Создано файлов: {len(generated_files)}")
    print(f"   Всего строк: {total_rows:,}")
    print(f"   Общий объем: {total_size / (1024 * 1024):.2f} MB")
    print(f"   Средний размер файла: {total_size / len(generated_files) / (1024 * 1024):.2f} MB")
    print(f"   Общее время: {total_duration:.2f} секунд")
    print(f"   Средняя скорость: {total_rows / total_duration:.0f} строк/сек")
    print(f"   Среднее время на файл: {total_duration / len(generated_files):.2f} сек")

    print(f"\n📁 ПАПКА С ДАННЫМИ: {os.path.abspath(output_directory)}")

    # Создаем файл с метаданными
    create_metadata_file(generated_files, files_to_generate, rows_per_file, total_duration, output_directory)

    print(f"\n📋 СПИСОК СОЗДАННЫХ ФАЙЛОВ:")
    if os.path.exists(output_directory):
        files = sorted([f for f in os.listdir(output_directory) if f.endswith('.csv')])
        for idx, file in enumerate(files, 1):
            filepath = os.path.join(output_directory, file)
            size = os.path.getsize(filepath)
            print(f"   {idx:2d}. {file} ({size:,} байт)")

    # Показываем путь к текущей директории
    print(f"\n📍 ТЕКУЩАЯ ДИРЕКТОРИЯ: {os.getcwd()}")