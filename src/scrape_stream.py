from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from typing import List, Dict, Any
import asyncio
from datetime import datetime, timedelta
import logging
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
# import pandas as pd
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

class MarketDataStream:
    def __init__(
        self,
        bootstrap_servers: List[str] = ['localhost:9092'],
        topic: str = 'ngx_market_data',
        batch_size: int = 100,
        scrape_interval: int = 1920  # 32 minutes in seconds
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.scrape_interval = scrape_interval
        self.producer = None
        self.base_url = "https://ngxgroup.com/exchange/data/equities-price-list/"
        self.driver = None
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def setup_driver(self):
        """Initialize Selenium WebDriver with Chrome options"""
        # chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Run in headless mode
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(10)
        self.logger.info("Selenium WebDriver initialized")

    def clean_numeric(self, value: str) -> float:
        """Clean numeric strings and convert to float"""
        try:
            return float(value.replace(',', '').strip())
        except (ValueError, AttributeError):
            return 0.0     
        
    def handle_cookie_popup(self):
        """Handle the cookie consent popup"""
        try:
            # Wait for cookie popup and find the accept button
            cookie_popup = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "cli-modal-content cli-bar-popup"))
            )
            accept_button = self.driver.find_element(By.CLASS_NAME,
                                                      "wt-cli-privacy-btn cli_setting_save_button wt-cli-privacy-accept-btn cli-btn")
            self.driver.execute_script("arguments[0].click();", accept_button)
            
            # Wait for popup to disappear
            WebDriverWait(self.driver, 20).until(
                EC.invisibility_of_element(cookie_popup)
            )
            self.logger.info("Cookie popup handled successfully")
        except Exception as e:
            self.logger.error(f"Error handling cookie popup: {e}")
            # Try to remove the cookie popup using JavaScript if clicking fails
            try:
                self.driver.execute_script("""
                    var element = document.getElementById('cookie-law-info-bar');
                    if(element) element.parentNode.removeChild(element);
                """)
                self.logger.info("Cookie popup removed via JavaScript")
            except Exception as js_error:
                self.logger.error(f"Failed to remove cookie popup: {js_error}")

    async def init_producer(self):
        """Initialize the Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            await self.producer.start()
            self.logger.info("Kafka producer initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise


    def scrape_table_data(self) -> List[ScrapeModel]:
            """Scrape data from the current page"""
            data_list = []
            try:
                # Wait for table to be present
                # table = WebDriverWait(self.driver, 10).until(
                #     EC.presence_of_element_located((By.ID, "ngx_equities_trading_statistics"))
                # )
                table = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "latestdiclosuresEquities")))
                
                # Get all rows except header
                rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                
                for row in rows:
                    try:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 11:
                            data = ScrapeModel(
                                company_name=cols[0].text.strip(),
                                previous_closing_price=self.clean_numeric(cols[1].text),
                                opening_price=self.clean_numeric(cols[2].text),
                                high=self.clean_numeric(cols[3].text),
                                low=self.clean_numeric(cols[4].text),
                                close=self.clean_numeric(cols[5].text),
                                change=self.clean_numeric(cols[6].text),
                                trade=int(self.clean_numeric(cols[7].text)),
                                volume=self.clean_numeric(cols[8].text),
                                value=self.clean_numeric(cols[9].text),
                                trade_date=cols[10].text.strip()
                            )
                            data_list.append(data)
                    except Exception as e:
                        self.logger.error(f"Error processing row: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error scraping table: {e}")
                
            return data_list
    
    async def scrape_data(self) -> List[ScrapeModel]:
        """Scrape market data with pagination"""
        all_data = []
        
        try:
            self.driver.get(self.base_url)
            time.sleep(5)  # Wait for initial page load
            
            # Handle cookie popup before proceeding
            self.handle_cookie_popup()
            
            while True:
                # Scrape current page
                page_data = self.scrape_table_data()
                all_data.extend(page_data)
                
                try:
                    # Wait for next button to be clickable
                    next_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "latestdiclosuresEquities_next"))
                    )
                    
                    if "disabled" in next_button.get_attribute("class"):
                        break
        
                    # # Use JavaScript to scroll the button into view and click it
                    # self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    # time.sleep(1)  # Brief pause after scrolling
                    # self.driver.execute_script("arguments[0].click();", next_button)
                    # time.sleep(2)  # Wait for new page to load
                    next_button.click()
                    time.sleep(5)  #
                    
                    
                except NoSuchElementException:
                    self.logger.info("No more pages to scrape")
                    break
                except Exception as e:
                    self.logger.error(f"Error navigating to next page: {e}")
                    break
            
            self.logger.info(f"Successfully scraped {len(all_data)} records")
            return all_data

        except Exception as e:
            self.logger.error(f"Error in scrape_data: {e}")
            return []

    async def send_to_kafka(self, data: List[ScrapeModel]):
        """Send scraped data to Kafka"""
        try:
            for record in data:
                record_dict = record.to_dict()
                if record_dict:  # Ensure it's not empty
                    await self.producer.send_and_wait(
                        self.topic,
                        value=record_dict
                    )
                    self.logger.info(f"Message sent to Kafka: {record_dict}")
                else:
                    self.logger.warning("Skipping empty record")
            self.logger.info(f"Sent {len(data)} records to Kafka topic {self.topic}")
        except Exception as e:
            self.logger.error(f"Error sending to Kafka: {str(e)}")
            raise

    async def stream_data(self):
        """Main streaming function"""
        self.setup_driver()
        await self.init_producer()
        
        try:
            while True:
                self.logger.info("Starting new scraping cycle")
                start_time = time.time()
                
                data = await self.scrape_data()
                
                if data:
                    await self.send_to_kafka(data)
                else:
                    self.logger.warning("No data scraped in this cycle")
                
                # Calculate time to wait until next cycle
                elapsed_time = time.time() - start_time
                wait_time = max(0, self.scrape_interval - elapsed_time)
                self.logger.info(f"Waiting {wait_time:.2f} seconds until next cycle")
                await asyncio.sleep(wait_time)
                
        except Exception as e:
            self.logger.error(f"Streaming error: {str(e)}")
        finally:
            await self.producer.stop()
            self.driver.quit()

async def main():
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'topic': 'ngx_market_data',
        'batch_size': 152,
        'scrape_interval': 1920  # 32 minutes
    }
    
    streamer = MarketDataStream(**kafka_config)
    
    try:
        await streamer.stream_data()
    except KeyboardInterrupt:
        logging.info("Shutting down streamer...")
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())


