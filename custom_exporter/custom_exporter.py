# custom_exporter.py
from prometheus_client import start_http_server, Gauge
import time, requests, random, logging, sys, os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    logging.error("OPENWEATHER_API_KEY not found in .env")
    sys.exit(1)

# Метрики с меткой city
weather_temperature = Gauge('weather_temperature_celsius', 'Current temperature', ['city', 'country'])
weather_feels_like = Gauge('weather_feels_like_celsius', 'Feels like temperature', ['city', 'country'])
weather_windspeed = Gauge('weather_windspeed_kmh', 'Current wind speed', ['city', 'country'])
weather_humidity = Gauge('weather_humidity_percent', 'Humidity level', ['city', 'country'])
weather_api_calls_total = Gauge('weather_api_calls_total', 'Total successful API calls')
weather_api_response_time = Gauge('weather_api_response_time_seconds', 'API response time in seconds')

# Курсы валют и BTC
g_fx_rate = Gauge('custom_fx_rate', 'Currency exchange rates', ['pair'])
g_crypto_price = Gauge('custom_crypto_price', 'Cryptocurrency prices', ['symbol'])

# Симулированные метрики
g_api_success = Gauge('custom_api_success', 'API success (1=ok,0=fail)')
g_active_users = Gauge('custom_sim_active_users', 'Simulated active users')
g_request_rate = Gauge('custom_sim_request_rate_per_min', 'Simulated request rate per minute')
g_random_load = Gauge('custom_sim_random_load', 'Random load metric')

# --- Список городов и координаты
cities = [
    {"name": "Astana", "lat": 51.1694, "lon": 71.4491},
    {"name": "Almaty", "lat": 43.2220, "lon": 76.8512},
    {"name": "Shymkent", "lat": 42.3417, "lon": 69.5901},
    {"name": "Karaganda", "lat": 49.8066, "lon": 73.0853},
    {"name": "Aktobe", "lat": 50.2839, "lon": 57.1668},
]

def fetch_weather_data():
    success_calls = 0
    for city in cities:
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': city['lat'],
                'lon': city['lon'],
                'appid': API_KEY,
                'units': 'metric'
            }

            start_time = time.time()
            response = requests.get(url, params=params, timeout=10)
            response_time = time.time() - start_time
            weather_api_response_time.set(response_time)

            response.raise_for_status()
            data = response.json()

            labels = {'city': city['name'], 'country': 'Kazakhstan'}

            weather_temperature.labels(**labels).set(data['main'].get('temp', 0))
            weather_feels_like.labels(**labels).set(data['main'].get('feels_like', 0))  
            weather_windspeed.labels(**labels).set(data.get('wind', {}).get('speed', 0))
            weather_humidity.labels(**labels).set(data['main'].get('humidity', 0))

            logging.info(f"✅ Weather data updated for {city['name']}")
            success_calls += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"⚠️ Weather API request failed for {city['name']}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error for {city['name']}: {e}")

    weather_api_calls_total.set(success_calls)

def fetch_fx():
    try:
        r = requests.get('https://open.er-api.com/v6/latest/USD', timeout=5)
        j = r.json()
        rates = j.get('rates', {})
        return rates.get('KZT'), rates.get('EUR')
    except Exception as e:
        print("FX fetch error:", e)
        return None, None

def fetch_btc_usd():
    try:
        r = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', timeout=5)
        j = r.json()
        return float(j['bitcoin']['usd'])
    except Exception as e:
        print("BTC error:", e)
        return None

if __name__ == '__main__':
    start_http_server(8000)
    print("Custom exporter running on :8000")

    while True:
        try:
            # --- Обновляем погоду по всем городам
            try:
                fetch_weather_data()
            except KeyboardInterrupt:
                logging.info("Exporter stopped manually.")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
            # --- Курсы валют
            usd_kzt, usd_eur = fetch_fx()
            if usd_kzt: g_fx_rate.labels(pair='USD_KZT').set(usd_kzt)
            if usd_eur: g_fx_rate.labels(pair='USD_EUR').set(usd_eur)

            # --- BTC
            btc_usd = fetch_btc_usd()
            if btc_usd: g_crypto_price.labels(symbol='BTC').set(btc_usd)

            # --- Симулированные метрики
            g_active_users.set(random.randint(100, 300))
            g_request_rate.set(random.uniform(50, 200))
            g_random_load.set(random.uniform(10, 90))
            g_api_success.set(1)

        except Exception as e:
            print("Error:", e)
            g_api_success.set(0)

        time.sleep(30)
