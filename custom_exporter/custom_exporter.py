from prometheus_client import start_http_server, Gauge
import time, requests, random

# 🔹 Метрики с лейблами
g_fx_rate = Gauge('custom_fx_rate', 'Currency exchange rates', ['pair'])
g_crypto_price = Gauge('custom_crypto_price', 'Cryptocurrency prices', ['symbol'])

# 🔹 Метрики погоды
g_temp_c = Gauge('custom_weather_temperature_c', 'Temperature (C) from Open-Meteo')
g_humidity = Gauge('custom_weather_humidity_pct', 'Relative humidity %')
g_windspeed = Gauge('custom_weather_windspeed_ms', 'Wind speed m/s')

# 🔹 Метрики симуляции
g_api_success = Gauge('custom_api_success', 'API success (1=ok,0=fail)')
g_active_users = Gauge('custom_sim_active_users', 'Simulated active users')
g_request_rate = Gauge('custom_sim_request_rate_per_min', 'Simulated request rate per minute')
g_random_load = Gauge('custom_sim_random_load', 'Random load metric')


# === Вспомогательные функции ===

def fetch_weather():
    """Получаем данные о погоде из Open-Meteo"""
    try:
        resp = requests.get(
            'https://api.open-meteo.com/v1/forecast?latitude=51.1605&longitude=71.4704&current_weather=true&hourly=relativehumidity_2m',
            timeout=5
        )
        data = resp.json()
        current = data.get('current_weather', {})
        temp = current.get('temperature')
        wind = current.get('windspeed')
        humidity = None
        try:
            hourly = data.get('hourly', {})
            rh = hourly.get('relativehumidity_2m', [])
            if rh:
                humidity = rh[0]
        except:
            humidity = None
        return temp, humidity, wind
    except Exception as e:
        print("Weather error:", e)
        return None, None, None


def fetch_btc_usd():
    """Получаем курс BTC→USD из CoinGecko"""
    try:
        r = requests.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd',
            timeout=5
        )
        j = r.json()
        price = j.get('bitcoin', {}).get('usd')
        return float(price)
    except Exception as e:
        print("BTC fetch error:", e)
        return None


def fetch_fx():
    """Получаем курсы USD→KZT и USD→EUR"""
    try:
        r = requests.get('https://open.er-api.com/v6/latest/USD', timeout=5)
        j = r.json()
        rates = j.get('rates', {})
        return rates.get('KZT'), rates.get('EUR')
    except Exception as e:
        print("FX fetch error:", e)
        return None, None

def fetch_eur_rates():
    """Получаем курсы EUR→USD и EUR→KZT"""
    try:
        r = requests.get('https://open.er-api.com/v6/latest/EUR', timeout=5)
        j = r.json()
        rates = j.get('rates', {})
        return rates.get('USD'), rates.get('KZT')
    except Exception as e:
        print("EUR FX fetch error:", e)
        return None, None

# === Основной цикл ===

if __name__ == '__main__':
    start_http_server(8000)
    print("✅ Custom exporter running on :8000")

    while True:
        try:
            # --- Погода ---
            temp, humidity, wind = fetch_weather()
            if temp is not None: g_temp_c.set(temp)
            if humidity is not None: g_humidity.set(humidity)
            if wind is not None: g_windspeed.set(wind)

            # --- Курсы валют ---
            usd_kzt, usd_eur = fetch_fx()
            if usd_kzt: g_fx_rate.labels(pair='USD_KZT').set(usd_kzt)
            if usd_eur: g_fx_rate.labels(pair='USD_EUR').set(usd_eur)

            eur_usd, eur_kzt = fetch_eur_rates()
            if eur_usd: g_fx_rate.labels(pair='EUR_USD').set(eur_usd)
            if eur_kzt: g_fx_rate.labels(pair='EUR_KZT').set(eur_kzt)


            # --- BTC курс ---
            btc_usd = fetch_btc_usd()
            if btc_usd: g_crypto_price.labels(symbol='BTC').set(btc_usd)

            # --- Симуляция ---
            g_active_users.set(random.randint(50, 300))
            g_request_rate.set(random.uniform(20, 400))
            g_random_load.set(random.random() * 100)

            g_api_success.set(1)

        except Exception as e:
            print("Main loop error:", e)
            g_api_success.set(0)

        time.sleep(20)
