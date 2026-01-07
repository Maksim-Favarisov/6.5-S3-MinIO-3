import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

from botocore.exceptions import ClientError
from watchfiles import awatch, Change
import pandas as pd
import aioboto3
import boto3

from config import MINIO_CONFIG


class MinIOPipeline:
    """
    Пайплайн для загрузки данных в MinIO
    """

    def __init__(self, config: dict):
        self.config = config
        self.setup_logging()

        # Инициализируем клиент MinIO
        self.minio_client = self.init_minio_client()

        # Для отслеживания обрабатываемых файлов
        self.processing_files = set()

        # Семафор для ограничения параллельных обработок
        self.semaphore = asyncio.Semaphore(
            config.get('max_concurrent_files', 3)
        )

        # Для временной агрегации логов
        self.last_log_upload_time = datetime.now()
        self.log_upload_interval = timedelta(minutes=1)  # Раз в 1 минуту
        self.logs_accumulated = False  # Флаг: есть ли новые логи с последней загрузки

        self.logger.info(f"Пайплайн инициализирован для MinIO: {config['endpoint_url']}")
        self.logger.info(f"Логи будут загружаться каждую минуту")

    def setup_logging(self):
        """Настройка логирования"""
        log_path = Path(self.config.get('log_file', './logs/minio_pipeline.log'))
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def init_minio_client(self):
        """
        Инициализация клиента для работы с MinIO
        """
        return boto3.client(
            's3',
            endpoint_url=self.config['endpoint_url'],  # URL MinIO
            aws_access_key_id=self.config['access_key'],
            aws_secret_access_key=self.config['secret_key'],
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )

    # ============================================================================
    # ЗАГРУЗКА ЛОГОВ
    # ============================================================================

    async def upload_log_file(self):
        """Загрузка лог-файла в MinIO"""
        try:
            log_path = Path(self.config['log_file'])

            # Загружаем файл
            s3_key = 'pipeline.log'

            async with await self.init_async_minio_client() as s3:
                await s3.put_object(
                    Bucket=self.config['bucket_name'],
                    Key=s3_key,
                    Body=open(log_path, 'rb'),
                    ContentType='text/plain'
                )

            self.logger.info(f"✅ Лог-файл загружен: {s3_key}")

            # Очищаем файл после загрузки
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('')

            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки: {e}")
            return False

    # ============================================================================
    # МЕТОД ДЛЯ ПРОВЕРКИ ВРЕМЕНИ И ЗАГРУЗКИ ЛОГОВ
    # ============================================================================

    async def check_and_upload_logs(self):
        """Проверить время и загрузить логи если прошла минута"""
        current_time = datetime.now()

        # Если прошла минута с последней загрузки и есть новые логи
        if (current_time - self.last_log_upload_time >= self.log_upload_interval
                and self.logs_accumulated):
            self.logger.info(f"📊 Загружаю логи (интервал: 1 минута)")
            await self.upload_log_file()

            # Сбрасываем флаги
            self.last_log_upload_time = current_time
            self.logs_accumulated = False
            return True

        return False

    # ============================================================================
    # ОСНОВНЫЕ МЕТОДЫ С ИНТЕГРАЦИЕЙ
    # ============================================================================

    async def init_async_minio_client(self):
        """
        Создание асинхронного клиента для MinIO
        """
        session = aioboto3.Session(
            aws_access_key_id=self.config['access_key'],
            aws_secret_access_key=self.config['secret_key']
        )

        return session.client(
            's3',
            endpoint_url=self.config['endpoint_url'],
            region_name='us-east-1'
        )

    def enable_bucket_versioning(self):
        """Включение версионирования для бакета"""

        versioning_status = self.config.get('bucket_versioning', '').strip().title()

        if versioning_status not in ['Enabled', 'Suspended']:
            self.logger.info(f"ℹ️  Версионирование не настраивается")
            return False

        try:
            self.minio_client.put_bucket_versioning(
                Bucket=self.config['bucket_name'],
                VersioningConfiguration={'Status': versioning_status}
            )

            self.logger.info(f"✅ Версионирование: {versioning_status}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"Ошибка: {error_code}")
            return False

    async def check_minio_connection(self):
        """Проверка подключения к MinIO"""
        try:
            response = self.minio_client.list_buckets()
            self.logger.info(f"✅ Подключение к MinIO успешно")

            self.enable_bucket_versioning()

            return True
        except Exception as e:
            self.logger.error(f"Не удалось подключиться к MinIO: {e}")
            return False

    async def ensure_bucket_exists(self, bucket_name: str):
        """Проверка существования бакета, создание если нет"""
        try:
            self.minio_client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"Бакет '{bucket_name}' существует")
            return True
        except:
            try:
                self.minio_client.create_bucket(Bucket=bucket_name)
                self.logger.info(f"Создан бакет '{bucket_name}'")
                return True
            except Exception as e:
                self.logger.error(f"Не удалось создать бакет: {e}")
                return False

    async def run(self):
        """Запуск основного цикла пайплайна"""
        self.logger.info("=" * 60)
        self.logger.info("ЗАПУСК ПАЙПЛАЙНА ДЛЯ MINIO")
        self.logger.info("Логи загружаются каждую минуту (если были события)")
        self.logger.info("=" * 60)

        if not await self.check_minio_connection():
            return

        if not await self.ensure_bucket_exists(self.config['bucket_name']):
            return

        # Запускаем фоновую задачу для периодической проверки времени
        background_task = asyncio.create_task(self.background_log_checker())

        try:
            self.create_folders()
            await self.monitor_folder()
        finally:
            # Останавливаем фоновую задачу
            background_task.cancel()

            # Загружаем финальные логи если есть
            if self.logs_accumulated:
                self.logger.info("Загружаю финальные логи...")
                await self.upload_log_file()

            self.logger.info("Пайплайн завершён")

    async def background_log_checker(self):
        """Фоновая задача: проверяем каждые 10 секунд, нужно ли загружать логи"""
        while True:
            try:
                await asyncio.sleep(10)  # Проверяем каждые 10 секунд
                await self.check_and_upload_logs()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Ошибка в фоновой задаче: {e}")

    def create_folders(self):
        """Создание необходимых локальных папок"""
        folders = [
            self.config['input_folder'],
            self.config['temp_folder'],
            self.config['archive_folder'],
            Path(self.config['log_file']).parent
        ]

        for folder in folders:
            Path(folder).mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Создана папка: {folder}")

    async def monitor_folder(self):
        """Мониторинг входной папки на новые файлы"""
        self.logger.info(f"Начинаю мониторинг папки: {self.config['input_folder']}")

        def csv_filter(change, path):
            file_path = Path(path)
            if file_path.is_dir():
                return False
            return file_path.suffix.lower() == '.csv'

        try:
            async for changes in awatch(
                    self.config['input_folder'],
                    watch_filter=csv_filter,
                    debounce=3000,
                    step=2000,
                    recursive=False
            ):
                await self.handle_changes(changes)

        except Exception as e:
            self.logger.error(f"Ошибка мониторинга: {e}")
            raise

    async def handle_changes(self, changes):
        """Обработка обнаруженных изменений"""
        for change_type, file_path_str in changes:
            file_path = Path(file_path_str)

            if change_type == Change.added:
                self.logger.info(f"Обнаружен новый CSV файл: {file_path.name}")
                # Запускаем обработку в отдельной задаче
                asyncio.create_task(self.process_file(file_path))

    async def process_file(self, file_path: Path):
        """Обработка файла. Отмечаем, что есть новые логи"""
        async with self.semaphore:
            if file_path in self.processing_files:
                return

            self.processing_files.add(file_path)

            try:
                self.logger.info(f"Начинаю обработку: {file_path.name}")

                if not await self.wait_for_file_stable(file_path):
                    return

                df = pd.read_csv(file_path)
                if df.empty:
                    return

                filtered_df = self.filter_data(df)

                temp_file = await self.save_temp_file(filtered_df, file_path)
                s3_key = await self.upload_to_minio(temp_file, file_path)
                await self.archive_source_file(file_path)
                temp_file.unlink()

                self.logger.info(f"✅ Файл обработан: {s3_key}")

                # ============================================
                # Отмечаем что есть новые логи
                # Не загружаем их сразу!
                # ============================================
                self.logs_accumulated = True

            except Exception as e:
                self.logger.error(f"❌ Ошибка: {e}")

                # Даже при ошибке отмечаем что есть логи
                self.logs_accumulated = True

            finally:
                self.processing_files.remove(file_path)

    async def wait_for_file_stable(self, file_path: Path, timeout: int = 30) -> bool:
        """Ожидание завершения записи файла"""
        import time

        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = file_path.stat().st_size

                if current_size > 0 and current_size == last_size:
                    await asyncio.sleep(2)
                    final_size = file_path.stat().st_size

                    if final_size == current_size:
                        return True

                last_size = current_size
                await asyncio.sleep(0.5)

            except FileNotFoundError:
                return False

        return False

    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Фильтрация данных по возрасту 18-40"""
        if 'age' not in df.columns:
            return df

        return df[(df['age'] >= 18) & (df['age'] <= 40)].copy()

    async def save_temp_file(self, df: pd.DataFrame, original_file: Path) -> Path:
        """Сохранение отфильтрованных данных во временный CSV файл"""
        temp_dir = Path(self.config['temp_folder'])
        temp_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_filename = f"filtered_{original_file.stem}_{timestamp}.csv"
        temp_file = temp_dir / temp_filename

        df.to_csv(temp_file, index=False)
        return temp_file

    async def upload_to_minio(self, file_path: Path, original_file: Path) -> str:
        """Асинхронная загрузка CSV файла в MinIO"""
        try:
            date_prefix = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"{self.config.get('s3_prefix', 'processed')}/{date_prefix}/{file_path.name}"

            async with await self.init_async_minio_client() as s3:
                await s3.upload_file(
                    Filename=str(file_path),
                    Bucket=self.config['bucket_name'],
                    Key=s3_key
                )

            return s3_key

        except Exception as e:
            self.logger.error(f"Ошибка загрузки в MinIO: {e}")
            raise

    async def archive_source_file(self, file_path: Path):
        """Перемещение исходного файла в архив"""
        try:
            archive_dir = Path(self.config['archive_folder'])

            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            date_structure = file_time.strftime("%Y/%m/%d")

            archive_path = archive_dir / date_structure / file_path.name
            archive_path.parent.mkdir(parents=True, exist_ok=True)

            file_path.rename(archive_path)

        except Exception as e:
            self.logger.error(f"Ошибка архивации: {e}")


# ============================================================================
# ЗАПУСК ПАЙПЛАЙНА
# ============================================================================

async def main():
    """Главная функция запуска"""

    try:
        print("=" * 60)
        print("DATA PIPELINE ДЛЯ MINIO")
        print("Логи загружаются каждую минуту, если были события")
        print("=" * 60)

        pipeline = MinIOPipeline(MINIO_CONFIG)
        await pipeline.run()

    except KeyboardInterrupt:
        print("\nПайплайн остановлен")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())