#Импорт библиотек 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime as dt
import zipfile, os, csv, re, logging
from clickhouse_driver import Client

# Базовые пути
ZIP_STAGING_DIR = "/home/danilagrinko/airflow/source_zips"
EXTRACTED_DIR = "/home/danilagrinko/airflow/extracted_csvs"
PROCESSED_CSV_DIR = "/home/danilagrinko/airflow/processed_csvs"
CLICKHOUSE_HOST = "localhost"

#Создание лога
log = logging.getLogger("airflow.task")

#Функция распаковки архивов
def extract_zip_files():
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    for fname in os.listdir(ZIP_STAGING_DIR):
        if fname.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(ZIP_STAGING_DIR, fname), 'r') as zip_ref:
                zip_ref.extractall(EXTRACTED_DIR)
                log.info(f"Extracted {fname}")

#Функция извлечения префикса из названия файла
def extract_prefix(filename: str, known_prefixes: list[str]) -> str | None:
    for prefix in known_prefixes:
        if re.match(rf"^{re.escape(prefix)}([_\W].*|\.csv$)", filename):
            return prefix
    return None

#Функция фильтрации, убираем строки с большим количеством пропусков 
def filter_sort_users(filename: str):
    users = []
    path = os.path.join(EXTRACTED_DIR, filename)

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"', skipinitialspace=True)
        next(reader, None)  # Пропускаем заголовки

        for line_num, fields in enumerate(reader, start=2):
            if len(fields) < 13:
                log.warning(f"⚠️ Строка {line_num} пропущена — слишком мало полей: {fields}")
                continue
            try:
                cleaned_key4 = re.sub(r':/everyday/.*$', '', fields[9]) #удаляем ненужные ссылки из parsedParamsKey4
                users.append({
                    "clientID": int(fields[0]),
                    "ipAddress": fields[10].replace('"', '').strip(),
                    "parsedParamsKey3": fields[8],
                    "parsedParamsKey4": cleaned_key4,
                    "dateTime": fields[12].strip(),
                    "deviceCategory": fields[2],
                })
            except Exception as e:
                log.error(f"❌ Ошибка в строке {line_num}: {e} → {fields}")
                continue

    return users


#функция сохранения обработанной таблицы
def save_to_csv(filename: str):
    data = filter_sort_users(filename)
    output_path = os.path.join(PROCESSED_CSV_DIR, f'{filename}_edited.csv')
#функция удаления лишних столбцов
    def clean(value: str) -> str:
        return value.replace('"', '').strip() if isinstance(value, str) else value

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["clientID", "ipAddress", "parsedParamsKey3",
                        "parsedParamsKey4", "dateTime", "deviceCategory"],
            quoting=csv.QUOTE_ALL,
            quotechar='"',
            escapechar='\\'
        )
        writer.writeheader()
        for row in data:
            cleaned_row = {
                "clientID": row["clientID"],
                "ipAddress": clean(row["ipAddress"]),
                "parsedParamsKey3": clean(row["parsedParamsKey3"]),
                "parsedParamsKey4": clean(row["parsedParamsKey4"]),
                "dateTime": clean(row["dateTime"]),
                "deviceCategory": clean(row["deviceCategory"]),
            }
            writer.writerow(cleaned_row)
#функция подсчета конверсий
def load_result(filename: str):
    res = []
    step1 = [
        'Product::Анкета Новая форма - Мобильный телефон::View',
        'Product::Анкета Новая форма - Электронная почта::View',
        'Product::Анкета Новая форма - ФИО::View', 'Product::Анкета Новая форма::View',
'Product::Анкета Новая форма - Гражданство РФ::View', 'Product::Оплата смартфоном::Click',
        'Product::SEO-блок::View', 'Product::Полезно знать::Click', 'Product::Тарифы::Click',
        'Product::SEO-блок - ссылки::Click', 'Product::Заказать карту::Click', 'Product::Кэшбэк::Click',
        'Product::Всё о�кэшбэке::Click', 'Product::Заказать карту::Click', 'Product::Карты::Menu_Click',
        'Product::Анкета Новая форма - ФИО в три поля::View', 'Product::Анкета Новая форма - Пол::View',
        'Product::test_event_dl::test_view', 'Product::Кредиты::Click', 'Product::Кредиты::View',
        'Product::Alfa Only::Menu_Click', 'Main Page::Футер::Click', 'Product::Расчётный счёт::Click',
        'Product::Приём платежей в торговых точках::Click', 'Product::Хэдер::Menu_Click',
        'Product::Меню сегментов::Menu_Click'
    ]

    step2 = ["Product::Анкета Новая форма - Гражданство РФ::Choice",
             'Product::Анкета Новая форма - ФИО::Enter',
             'Product::Анкета Новая форма - Электронная почта::Enter',
             'Product::Анкета Новая форма - Дата рождения::Enter',
             'Product::Анкета Новая форма - Пол::Choice',
             'Product::Анкета Новая форма - Мобильный телефон::Enter',
             'Product::Анкета Новая форма - Отчество::Enter',
             'Product::Анкета Новая форма - По паспорту без отчества::Choice',
             'Product::Анкета Новая форма - Фамилия::Enter',
             'Product::Анкета Новая форма - Имя::Enter',
             ]
    step3 = ['Product::Анкета - Шаг 1::Send', 'Product::Pop-up с СМС кодом::View',
             'Product::Pop-up с СМС кодом - Изменить номер телефона::View',
             'Product::Pop-up с СМС кодом::Enter', 'Product::Кажется мы уже знакомы::View',
             'Product::Pop-up с СМС кодом ::Enter',
             'Product::Серия и номер паспорта::Enter', 'Product::Серия и номер паспорта::Click',
             'Product::Pop-up с СМС кодом - Ввести код больше нельзя::View',
             'Product::Pop-up с СМС кодом - Ввести код больше нельзя::Click', 'Product::Pop-up с СМС кодом::View'
             ]

    step4 = ['Product::Заказ доставки::View', 'Product::Адрес доставки::Enter',
             'Product::Куда доставить::View',
             'Product::Выберите офис банка::View',
             'Product::Дата и время::View', 'Product::Дата и время::Click',
             'Product::Куда доставить::Choice',
             'Product::Комментарий в доставке по адресу::Enter',
             'Product::Заказ доставки - подтверждение СМС::View',
             'Product::Заказ доставки - подтверждение СМС::Enter',
             'Product::Заказ доставки - подтверждение СМС ::Enter',
             ]

    step5 = ['Product::Мобильное меню_вход в АЛБО::Click',
             'Product::Мобильное меню_вход в АО::Click', ]

    users = filter_sort_users(filename)
    client1, client2, client3, client4, client5 = set(), set(), set(), set(), set()

    for user in users:
        if user["parsedParamsKey3"] in step1: client1.add(user["clientID"])
        if user["parsedParamsKey3"] in step2: client2.add(user["clientID"])
        if user["parsedParamsKey3"] in step3: client3.add(user["clientID"])
        if user["parsedParamsKey3"] in step4: client4.add(user["clientID"])
        if user["parsedParamsKey3"] in step5: client5.add(user["clientID"])

    x1, x2, x3, x4, x5 = len(client1), len(client2), len(client3), len(client4), len(client5)

    def safe_div(a, b): return a / b if b else 0.0
    res.append({
        "visitors": x1, "step2": x2, "step3": x3, "step4": x4, "step5": x5,
        "conv_visit_to_step2": safe_div(x2, x1),
        "conv_step2_to_step3": safe_div(x3, x2),
        "conv_step3_to_step4": safe_div(x4, x3),
        "conv_step4_to_step5": safe_div(x5, x4),
        "conv_step1_to_step4": safe_div(x4, x1)
    })

    with open(os.path.join(PROCESSED_CSV_DIR, f'{filename}_conv.csv'), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "visitors", "step2", "step3", "step4", "step5",
            "conv_visit_to_step2", "conv_step2_to_step3",
            "conv_step3_to_step4", "conv_step4_to_step5",
            "conv_step1_to_step4"
        ])
        writer.writeheader()
        for row in res:
            writer.writerow(row)
#функция переброски csv в таблицы Clickhouse
def insert_csv_file():
    client = Client(host=CLICKHOUSE_HOST, user='default', password= '1234')
    table_map = Variable.get("csv_table_map_full", deserialize_json=True)
    inserted_files = 0

    for fname in os.listdir(PROCESSED_CSV_DIR):
        if "_edited.csv" not in fname and "_conv.csv" not in fname:
            continue


        match = re.match(r"^(.*)\.csv_(edited|conv)\.csv$", fname)
        if not match:
            continue

        prefix = match.group(1)  # например, travel
        filetype = match.group(2)  # либо edited либо conv

        try:
            table_name = table_map[prefix][filetype]
        except KeyError:
            log.warning(f"Не найдена таблица для {prefix} / {filetype}")
            continue

        full_path = os.path.join(PROCESSED_CSV_DIR, fname)
        batch = []
        with open(full_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)

            for row in reader:
                try:
                    if filetype == "edited":
                        if len(row) != 6:
                            log.error(f"Неверное число колонок в {fname}: {row}")
                            continue
                        try:
                            parsed = (
                                int(row[0]),
                                row[1],
                                [row[2]] if row[2] else [],
                                [row[3]] if row[3] else [],
                                dt.strptime(row[4].split('.')[0], "%Y-%m-%d %H:%M:%S"),
                                row[5],
                            )
                        except Exception as e:
                            log.error(f"Ошибка в {fname}: {row} → {e}")
                            continue
                    else:
                        parsed = (
                            int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]),
                            float(row[5]), float(row[6]), float(row[7]), float(row[8]), float(row[9])
                        )
                    batch.append(parsed)

                    if len(batch) >= 1000:
                        client.execute(f"INSERT INTO {table_name} VALUES", batch)
                        batch = []
                except Exception as e:
                    log.error(f"Ошибка в {fname}: {row} → {e}")

        if batch:
            client.execute(f"INSERT INTO {table_name} VALUES", batch)

        inserted_files += 1
    log.info(f"Обработано файлов: {inserted_files}")

# Главная объединяющая функция
def full_pipeline():
    extract_zip_files()
    table_map = Variable.get("csv_table_map_full", deserialize_json=True)
    known_prefixes = list(table_map.keys())
    for fname in os.listdir(EXTRACTED_DIR):
        if not fname.endswith(".csv"):
            continue
        prefix = extract_prefix(fname, known_prefixes)
        if prefix is None:
            continue
        save_to_csv(fname)
        load_result(fname)
    insert_csv_file()

# DAG
default_args = {"start_date": dt(2023, 1, 1)}
with DAG("new_upload", schedule_interval=None, catchup=False, default_args=default_args) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    full_task = PythonOperator(
        task_id="full_processing",
        python_callable=full_pipeline
    )

    begin >> full_task >> end


