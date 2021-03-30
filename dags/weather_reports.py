from datetime import datetime, timedelta, date
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
import pandas
import pendulum
import requests


key = os.environ['WEATHER_API_KEY']
local_tz = pendulum.timezone("Europe/Kiev")


city_queries = {
    "Cherkasy": "Черкаси",
    "Chernihiv": "Чернігів",
    "Chernivtsi": "Чернівці",
    "Simferopol": "Сімферополь",
    "Dnipropetrovsk": "Дніпро",
    "Donetsk,Donets%27ka": "Донецьк",
    "IvanoFrankivsk": "Івано-Франківськ",
    "Kharkiv": "Харків",
    "Kherson": "Херсон",
    "Khmelnytskyy": "Хмельницький",
    "Kiev": "Київ",
    "Kirovograd": "Кропивницький",
    "Luhansk": "Луганськ",
    "Lviv": "Львів",
    "Mykolayiv,Mykolayivs%27ka": "Миколаїв",
    "Odessa": "Одеса",
    "Poltava": "Полтава",
    "Rivne": "Рівне",
    "Sumy": "Суми",
    "Ternopil": "Тернопіль",
    "Vinnitsa": "Вінниця",
    "Lutsk": "Луцьк",
    "Uzhgorod": "Ужгород",
    "Zaporizhzhya": "Запоріжжя",
    "Zhitomir": "Житомир"
}


default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 3, 28, tzinfo=local_tz),
    "retries": 10,
    "retry_delay": timedelta(minutes=5)
}


def get_weather(query):
    indata = requests.get("http://api.weatherstack.com/current?access_key={}&query={}".format(key, query)).json()
    temp = indata["current"]["temperature"]
    humidity = indata["current"]["humidity"]
    return (temp, humidity)


def create_report(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids=["get_weather_{}".format(city) for city in city_queries.values()])
    df = pandas.DataFrame(data, columns=["temperature", "humidity"])
    df["city"] = city_queries.values()
    today_date = str(date.today())
    df["date"] = today_date
    df = df[["city", "temperature", "humidity", "date"]]
    df = df.sort_values("city").reset_index(drop=True)
    df.to_csv("ukraine_weather_report_{}.csv".format(today_date), index=False)
    df = pandas.read_csv("ukraine_weather_report_{}.csv".format(today_date))


with DAG(dag_id="weather_data_pipeline", schedule_interval="0 12 * * *", default_args=default_args, catchup=False) as dag:
    
    is_weather_api_available = HttpSensor(
        task_id="is_weather_api_available",
        method="GET",
        http_conn_id="weather_api_conn",
        endpoint="current?access_key={}&query=Kiev".format(key),
        response_check=lambda response: "request" in response.json(),
        poke_interval=5,
        timeout=20
    )

    _create_report = PythonOperator(
        task_id="create_report",
        python_callable=create_report,
        provide_context=True,
        dag=dag
    )

    for query in city_queries.keys():
        task_id = "get_weather_{}".format(city_queries[query])

        _get_weather = PythonOperator(
            task_id=task_id,
            op_kwargs={"query": query},
            python_callable=get_weather,
            dag=dag
        )

        is_weather_api_available >> _get_weather

        _get_weather >> _create_report