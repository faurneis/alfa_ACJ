import pandas as pd
import csv

import re

def filter_sort_users():
    users = []
    with open("travel.csv", "r", encoding="utf-8") as f:
        next(f)
        for line_num, line in enumerate(f, start=2):
            fields = line.strip().split(",")

            if len(fields) < 13:
                print(f"⚠️ Строка {line_num} пропущена — слишком мало полей ({len(fields)})")
                continue

            try:
                cleaned_key4 = re.sub(r':/everyday/.*$', '', fields[9])

                users.append({
                    "clientID": int(fields[0]),
                    "regionCity": fields[4],
                    "parsedParamsKey3": fields[8],
                    "parsedParamsKey4": cleaned_key4,
                    "dateTime": fields[12]
                })
            except ValueError as e:
                print(f"❌ Ошибка в строке {line_num}: {e}")
                continue

    # for i, user in enumerate(users):
    #     if i < 10:
    #         print(user)
    #     else:
    #         break
    return users


def save_to_csv():
    data = filter_sort_users()

    with open("out.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f,
                                fieldnames=["clientID", 'browser', 'deviceCategory', "regionCity", "parsedParamsKey3",
                                            "parsedParamsKey4", "ipAddress", "dateTime", "lastTrafficSource"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print("out.csv сохранён")


save_to_csv()
events_step1 = [
    'Product::Анкета Новая форма - Мобильный телефон::View', 'Product::Анкета Новая форма - Электронная почта::View',
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

events_step3 = ['Product::Анкета - Шаг 1::Send', 'Product::Pop-up с СМС кодом::View', 'Product::Pop-up с СМС кодом - Изменить номер телефона::View',
                'Product::Pop-up с СМС кодом::Enter', 'Product::Кажется мы уже знакомы::View','Product::Pop-up с СМС кодом ::Enter',
                'Product::Серия и номер паспорта::Enter', 'Product::Серия и номер паспорта::Click','Product::Pop-up с СМС кодом - Ввести код больше нельзя::View',
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
        'Product::Мобильное меню_вход в АО::Click',]


def count_unique_clients_step(users, target_events):
    matched_client_ids = set()

    for user in users:
        if user["parsedParamsKey3"] in target_events:
            matched_client_ids.add(user["clientID"])

    return len(matched_client_ids)

data = filter_sort_users()
print(count_unique_clients_step(data, events_step1))
print(count_unique_clients_step(data, step2))
print(count_unique_clients_step(data, events_step3))
print(count_unique_clients_step(data, step4))
print(count_unique_clients_step(data, step5))

df = pd.read_csv("travel.csv")

df.head()

df["parsedParamsKey3"].unique()

df['parsedParamsKey4'] = df['parsedParamsKey4'].str.replace(r':/everyday/.*$', '', regex=True)
