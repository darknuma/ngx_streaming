from prefect import flow, task
from kafka.errors import KafkaError
import json
from typing import List, Dict, Any
import asyncio
from datetime import datetime, timedelta
import logging
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time


class ScrapeModel(BaseModel):
    company_name: str
    previous_closing_price: float
    opening_price: float
    high: float
    low: float
    close: float
    change: float
    trade: float
    volume: float
    value: float
    trade_date: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.model_dump()
        }


@task
def setup_driver():
    """Initialize Selenium WebDriver with Chrome options"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    logging.info("Selenium WebDriver initialized")
    return driver


@task
def clean_numeric(value: str) -> float:
    """Clean numeric strings and convert to float"""
    try:
        return float(value.replace(',', '').strip())
    except (ValueError, AttributeError):
        return 0.0


@task
def scrape_table_data(driver) -> List[ScrapeModel]:
    """Scrape data from the current page"""
    data_list = []
    try:
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "latestdiclosuresEquities"))
        )
        rows = table.find_elements(By.TAG_NAME, "tr")[1:]
        for row in rows:
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 11:
                    data = ScrapeModel(
                        company_name=cols[0].text.strip(),
                        previous_closing_price=clean_numeric(cols[1].text),
                        opening_price=clean_numeric(cols[2].text),
                        high=clean_numeric(cols[3].text),
                        low=clean_numeric(cols[4].text),
                        close=clean_numeric(cols[5].text),
                        change=clean_numeric(cols[6].text),
                        trade=int(clean_numeric(cols[7].text)),
                        volume=clean_numeric(cols[8].text),
                        value=clean_numeric(cols[9].text),
                        trade_date=cols[10].text.strip(),
                    )
                    data_list.append(data)
            except Exception as e:
                logging.error(f"Error processing row: {e}")
                continue
    except Exception as e:
        logging.error(f"Error scraping table: {e}")

    return data_list


@task(cache_policy=None)
async def init_producer(bootstrap_servers: List[str]):
    """Initialize the Kafka producer"""
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        await producer.start()
        logging.info("Kafka producer initialized")
        return producer
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        raise


@task(cache_policy=None)
async def send_to_kafka(producer, topic, data: List[ScrapeModel]):
    """Send scraped data to Kafka"""
    try:
        for record in data:
            record_dict = record.to_dict()
            if record_dict:  # Ensure it's not empty
                await producer.send_and_wait(
                    topic,
                    value=record_dict
                )
                logging.info(f"Message sent to Kafka: {record_dict}")
            else:
                logging.warning("Skipping empty record")
        logging.info(f"Sent {len(data)} records to Kafka topic {topic}")
    except Exception as e:
        logging.error(f"Error sending to Kafka: {str(e)}")
        raise


@flow
async def market_data_stream(
    bootstrap_servers: List[str] = ['localhost:9092'],
    topic: str = 'ngx_market_data',
    batch_size: int = 100,
    scrape_interval: int = 1920  # 32 minutes in seconds
):
    """Main Prefect Flow to scrape data and send to Kafka"""
    driver = setup_driver()
    producer = await init_producer(bootstrap_servers)

    try:
        while True:
            logging.info("Starting new scraping cycle")
            start_time = time.time()

            # Navigate to the base URL
            base_url = "https://ngxgroup.com/exchange/data/equities-price-list/"
            driver.get(base_url)
            time.sleep(5)  # Wait for the page to load

            # Scrape data
            data = scrape_table_data(driver)

            # Send data to Kafka
            if data:
                await send_to_kafka(producer, topic, data)
            else:
                logging.warning("No data scraped in this cycle")

            # Wait for the next cycle
            elapsed_time = time.time() - start_time
            wait_time = max(0, scrape_interval - elapsed_time)
            logging.info(f"Waiting {wait_time:.2f} seconds until next cycle")
            await asyncio.sleep(wait_time)

    except KeyboardInterrupt:
        logging.info("Shutting down streamer...")
    except Exception as e:
        logging.error(f"Streaming error: {str(e)}")
    finally:
        await producer.stop()
        driver.quit()


if __name__ == "__main__":
    asyncio.run(market_data_stream())

