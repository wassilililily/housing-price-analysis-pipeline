from airflow.decorators import task
from selenium import webdriver
from selenium_stealth import stealth
from selenium.webdriver.common.by import By
import time

@task(retries=0)
def extract_propertyguru():
    def init_driver():
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=options)

        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)

        return driver
 
    def scrape_one_page():
        property_for_sale_url = "https://www.propertyguru.com.sg/property-for-sale"
        listings = []

        driver.get(property_for_sale_url)
        # Let the page load
        time.sleep(5)

        listing_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-listing-id]')

        for listing_element in listing_elements:
            listing = scrape_listing(listing_element)
            listings.append(listing)

        return listings
    
    def scrape_listing(listing_element):
        # Helper function to return the default value if cannot be found
        def safe_find_element(selector, parent,  by=By.CSS_SELECTOR, default=None):
            try:
                return parent.find_element(by, selector).text
            except:
                return default

        def safe_find_all(selector, parent, by=By.CSS_SELECTOR, default=[]):
            try:
                return parent.find_elements(by, selector)
            except:
                return default
        
        gallery_element = None # Unused
        description_element = listing_element.find_element(By.CSS_SELECTOR, 'div[da-id="lc-details-div"]')
        agent_element = listing_element.find_element(By.CSS_SELECTOR, 'div[da-id="lc-agent"]')

        listing_id = listing_element.get_attribute("data-listing-id")
        listing_title = safe_find_element('h3[da-id="lc-title"]', description_element)
        listing_address = safe_find_element('div[da-id="lc-address"]', description_element)
        listing_price = safe_find_element('div[da-id="lc-price"]', description_element)
        listing_time = safe_find_element('ul[da-id="lc-recency"] span[da-id="lc-feature-info"]', description_element)

        details_elements = safe_find_all('span[da-id="lc-feature-info"]', description_element)
        details = []
        # Always in this order
        details.append(details_elements[0].text + " bedrooms")
        details.append(details_elements[1].text + " bathrooms")
        # Remaining may have up to many detail badges
        for elem in details_elements[2:]:
            details.append(elem.text)
        
        info_elements = safe_find_all('div.listing-property-group span[da-id="lc-info-badge"]', description_element)
        info = []
        for elem in info_elements:
            info.append(elem.text)

        agent_description = safe_find_element('div[da-id="lc-agent-description"]', agent_element)
        
        return {
            "id": listing_id,
            "title": listing_title,
            "address": listing_address,
            "price": listing_price,
            "agent_description": agent_description,
            "details": details,
            "info": info,
        }

    driver = init_driver()
    listings = scrape_one_page()

    return listings