import os
import asyncio
import aiohttp
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

SNAPSHOT_INTERVAL = 600
API_BASE = "https://www.okx.com"
CURRENCIES = ["BTC", "ETH"]


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


async def fetch_json(session, url):
    try:
        async with session.get(url, timeout=30) as resp:
            if resp.status != 200:
                logging.error(f'HTTP {resp.status} при запросе к {url}')
                return []
            data = await resp.json()
            if 'data' not in data:
                logging.error(f'Нет поля data для {url}. Есть: {list(data.keys())}')
                return []
            return data['data']
    except asyncio.TimeoutError:
        logging.exception(f'Таймаут {url} - сервер не отвечает')
        return []
    except aiohttp.ClientConnectionError:
        logging.exception(f'Ошибка соединения с {url}')
        return []
    except Exception as e:
        logging.exception(f'Неожиданная ошибка при запросе к {url}')
        return []


async def fetch_currency_data(session, currency):
    opt_summary_url = f"{API_BASE}/api/v5/public/opt-summary?uly={currency}-USD"
    mark_price_url = f"{API_BASE}/api/v5/public/mark-price?instType=OPTION&uly={currency}-USD"

    summary_task = fetch_json(session, opt_summary_url)
    price_task = fetch_json(session, mark_price_url)

    summary_data, price_data = await asyncio.gather(summary_task, price_task)
    price_map = {p["instId"]: p["markPx"] for p in price_data} if price_data else None

    return summary_data, price_map


def build_rows(summary_data, price_map):
    rows = []
    for item in summary_data:
        inst = item["instId"]
        snapshot_ts = datetime.fromtimestamp(float(item['ts']) / 1000.0)
        price = price_map.get(inst) if price_map else None

        row = (
            snapshot_ts,
            inst,
            round(float(price), 2) if price else None,
            round(float(item.get("fwdPx")), 2) if item.get("fwdPx") else None,
            round(float(item.get("askVol")), 3) if item.get("askVol") else None,
            round(float(item.get("bidVol")), 3) if item.get("bidVol") else None,
            round(float(item.get("markVol")), 3) if item.get("markVol") else None,
            round(float(item.get("volLv")), 3) if item.get("volLv") else None,
            round(float(item.get("realVol")), 3) if item.get("realVol") else None,
            float(item.get("delta")) if item.get("delta") else None,
            float(item.get("deltaBS")) if item.get("deltaBS") else None,
            float(item.get("gamma")) if item.get("gamma") else None,
            float(item.get("gammaBS")) if item.get("gammaBS") else None,
            float(item.get("vega")) if item.get("vega") else None,
            float(item.get("vegaBS")) if item.get("vegaBS") else None,
            float(item.get("theta")) if item.get("theta") else None,
            float(item.get("thetaBS")) if item.get("thetaBS") else None,
            round(float(item.get("distance")), 3) if item.get("distance") else None,
            round(float(item.get("lever")), 2) if item.get("lever") else None,
            round(float(item.get("buyApr")), 2) if item.get("buyApr") else None,
            round(float(item.get("sellApr")), 2) if item.get("sellApr") else None
        )
        rows.append(row)
    return rows


def insert_rows(rows):
    query = """
        INSERT INTO option_snapshots (
            snapshot_ts,
            instrument_name,
            mark_price,
            forward_price,
            askVol,
            bidVol,
            markVol,
            volLv,
            realVol,
            delta,
            deltaBS,
            gamma,
            gammaBS,
            vega,
            vegaBS,
            theta,
            thetaBS,
            distance,
            leverage,
            buyApr,
            sellApr
        )
        VALUES %s
    """
    conn = get_db_connection()
    cur = None

    try:
        cur = conn.cursor()
        execute_values(cur, query, rows)
        conn.commit()
        logging.info(f"Вставка {len(rows)} строк в БД")

    except psycopg2.Error as e:
        logging.exception("Ошибка при вставке данных в БД")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


async def snapshot_loop():
    async with aiohttp.ClientSession() as session:
        all_rows = []

        for currency in CURRENCIES:
            summary, prices = await fetch_currency_data(session, currency)
            if summary:
                try:
                    rows = build_rows(summary, prices)
                    logging.info(f"Получено {len(rows)} строк по {currency}")
                    all_rows.extend(rows)
                except Exception as e:
                    logging.exception(f"Ошибка при формировании данных по {currency}")

        if all_rows:
            try:
                insert_rows(all_rows)
            except Exception:
                logging.exception("Ошибка записи snapshot в БД")


async def main():
    while True:
        await snapshot_loop()
        await asyncio.sleep(SNAPSHOT_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())