import requests
import pandas as pd
import dotenv
import logging
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
dotenv.load_dotenv()

# XCom

API_KEY = os.getenv("API_KEY")
BASE_URL = "http://api.weatherstack.com/current"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
locations = ["Lagos", "Abuja", "Kano", "Ibadan", "Kaduna"]


def extract(**kwargs):
    weather_data = []
    try:
       for location in locations:
           
           params = {
               "access_key": API_KEY,
               "query": location
           }
           response = requests.get(BASE_URL, params=params)

           if response.status_code == 200:
                data = response.json()
                weather_data.append(data)
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        logging.error(f"Error fetching weather data: {e}")
    finally:
        if len(weather_data) == 0:
            logging.error("No weather data fetched")
        else:
            print("Weather data fetched successfully")
            logging.info("Weather data fetched successfully")

    ti = kwargs['ti']
    ti.xcom_push(key="weather_data", value=weather_data)
    return weather_data

def transform(**kwargs):
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(key="weather_data", task_ids="extract_weather_data")
    transformed_data = []
    try:
        for data in weather_data:
            transformed_data.append({
                "location": data["location"]["name"],
                "temperature": data["current"]["temperature"],
                "wind_speed": data["current"]["wind_speed"],
                "pressure": data["current"]["pressure"],
                "humidity": data["current"]["humidity"],
                "local_time": data["location"]["localtime"],
                "time_zone": data["location"]["timezone_id"]
        })
    except Exception as e:
        print(f"Error transforming weather data: {e}")
        logging.error(f"Error transforming weather data: {e}")
    finally:
        if len(transformed_data) == 0:
            logging.error("No transformed data")
        else:
            print("Weather data transformed successfully")
            logging.info("Weather data transformed successfully")
    transformed_data = pd.DataFrame(transformed_data)

    ti.xcom_push(key="transformed_data", value=transformed_data)

    return transformed_data

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True)
    location = Column(String(255))
    temperature = Column(Float)
    wind_speed = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)    
    local_time = Column(DateTime)
    time_zone = Column(String(255))

def load(**kwargs):
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    Session = sessionmaker(bind=engine)
    session = Session()

    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_weather_data")

    Base.metadata.create_all(engine)

    try: 
        data_to_insert = transformed_data.to_dict(orient="records")
        session.bulk_insert_mappings(WeatherData, data_to_insert)
        session.commit()

    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error loading data: {e}")
        logging.error(f"Error loading data: {e}")
    finally:
        session.close()
        print("Data loaded successfully")
        logging.info("Data loaded successfully")