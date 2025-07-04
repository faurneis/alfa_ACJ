Airflow Data Processing Pipeline

Пайплайн для обработки данных веб-аналитики и расчета метрик конверсии воронки продаж.

Описание

Данный DAG обрабатывает данные пользовательской активности, извлекает их из ZIP-архивов, очищает, анализирует воронку конверсии и загружает результаты в ClickHouse.

Архитектура:
ZIP архивы → Извлечение → Очистка данных → Анализ конверсии → ClickHouse

Этапы обработки:

1. Извлечение - распаковка ZIP файлов с CSV данными
2. Фильтрация - очистка и валидация данных пользователей
3. Анализ - расчет метрик конверсии по 5-шаговой воронке
4. Загрузка - вставка данных в таблицы ClickHouse

Структура проекта

```
/home/danilagrinko/airflow/
├── source_zips/          # Исходные ZIP архивы
├── extracted_csvs/       # Распакованные CSV файлы
├── processed_csvs/       # Обработанные данные
└── dags/                 # DAG файлы
```

Конфигурация

Переменные Airflow

Необходимо настроить переменную Variable `csv_table_map_full` в формате JSON:

```json
{
  "travel": {
    "edited": "analytics.travel_users",
    "conv": "analytics.travel_conversion"
  }
}

Настройки подключения

- **ClickHouse Host**: `localhost`
- **ClickHouse User**: `default`
- **ClickHouse Password**: `1234` 

Формат данных

Входные CSV файлы

Ожидаемые колонки (минимум 13):
- `[0]` - clientID (int)
- `[1]` - строковое поле
- `[2]` - deviceCategory
- `[8]` - parsedParamsKey3 (события)
- `[9]` - parsedParamsKey4 (URL/пути)
- `[10]` - ipAddress
- `[12]` - dateTime

Выходные файлы

Файлы `*_edited.csv`:
```csv
clientID,ipAddress,parsedParamsKey3,parsedParamsKey4,dateTime,deviceCategory
```

Файлы `*_conv.csv`:
```csv
visitors,step2,step3,step4,step5,conv_visit_to_step2,conv_step2_to_step3,conv_step3_to_step4,conv_step4_to_step5,conv_step1_to_step4
```

Воронка конверсии

Step 1: Просмотр продукта
- Просмотр анкеты, форм, продуктовых страниц
- События типа `Product::*::View`, `Product::*::Click`

Step 2: Заполнение данных
- Ввод личных данных в формы
- События типа `Product::Анкета Новая форма - *::Enter`

Step 3: Отправка и верификация
- Отправка формы, подтверждение SMS
- События типа `Product::Анкета - Шаг 1::Send`

Step 4: Настройка доставки
- Выбор адреса, времени доставки
- События типа `Product::Заказ доставки::*`

Step 5: Авторизация
- Вход в мобильное приложение
- События типа `Product::Мобильное меню_вход в *::Click`

## Использование

### Запуск DAG

```bash
# Ручной запуск
airflow dags trigger proba

# Через веб-интерфейс
http://localhost:8080/admin/airflow/graph?dag_id=proba
```

### Подготовка данных

1. Поместите ZIP архивы в `/home/danilagrinko/airflow/source_zips/`
2. Убедитесь, что имена файлов содержат известные префиксы
3. Настройте переменную `csv_table_map_full`

### Мониторинг

- Логи доступны в веб-интерфейсе Airflow
- Ошибки обработки записываются в лог с префиксом `❌`
- Предупреждения о пропущенных строках помечены `⚠️`

## Структура таблиц ClickHouse

### Таблицы для edited данных
```sql
CREATE TABLE analytics.{prefix}_users (
    clientID Int32,
    ipAddress String,
    parsedParamsKey3 Array(String),
    parsedParamsKey4 Array(String), 
    dateTime DateTime,
    deviceCategory String
) ENGINE = MergeTree()
ORDER BY (clientID, dateTime);
```

### Таблицы для conv данных
```sql
CREATE TABLE analytics.{prefix}_conversion (
    visitors Int32,
    step2 Int32,
    step3 Int32,
    step4 Int32,
    step5 Int32,
    conv_visit_to_step2 Float64,
    conv_step2_to_step3 Float64,
    conv_step3_to_step4 Float64,
    conv_step4_to_step5 Float64,
    conv_step1_to_step4 Float64
) ENGINE = MergeTree()
ORDER BY visitors;
```


## Требования

- Python 3.8+
- Apache Airflow 2.0+
- ClickHouse client
- Зависимости: `clickhouse-driver`

