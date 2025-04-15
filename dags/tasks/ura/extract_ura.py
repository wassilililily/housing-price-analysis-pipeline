import os
import time
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = webdriver.ChromeOptions()
options.add_argument("start-maximized")
options.add_experimental_option("detach", True)
url = "https://eservice.ura.gov.sg/property-market-information/pmiResidentialTransactionSearch#"

def get_driver():
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    return driver

def select_district_and_property_type(driver, district_value, property_type_value):
    wait = WebDriverWait(driver, 10)

    # Open the modal for district selection
    modal_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.search-popup-btn")))
    modal_button.click()
    time.sleep(2)  # Allow modal to appear

    # Switch to the "Postal District" tab
    postal_district_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@href='#postalDistrict']")))
    postal_district_tab.click()
    time.sleep(1)  # Allow tab switch

    # Select the district checkbox using its value attribute
    district_checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, f"//input[@type='checkbox' and @value='{district_value}']")))
    driver.execute_script("arguments[0].click();", district_checkbox)  # Ensure checkbox is clicked

    # Click "Apply" button to confirm selection
    apply_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Apply')]")))
    apply_button.click()
    time.sleep(2)

    # Select the property type
    property_select = wait.until(EC.element_to_be_clickable((By.ID, "propertyTypeGroupNo")))
    property_select.click()
    property_option = wait.until(EC.element_to_be_clickable((By.XPATH, f"//option[@value='{property_type_value}']")))
    property_option.click()
    time.sleep(1)

    # Click the search button
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(@class, 'btn-primary')]")))
    search_button.click()

    check_and_process(driver)

    # Wait for results to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "resultAnalysisList1"))
    )
    # Return page source or driver for further extraction
#     return driver

    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[@class='btn btn-default dropdown-toggle']"))
        )
        # Click the dropdown to reveal the CSV option
    download_button.click()

        # Wait for the CSV link to be clickable
    csv_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'CSV')]"))
        )
    # Click the CSV link to initiate the download
    csv_link.click()
    time.sleep(5) 
    print ("File saved")
    return driver

def click_back_button(driver):
    wait = WebDriverWait(driver, 10)
    back_button = wait.until(EC.element_to_be_clickable((By.ID, "back")))
    back_button.click()

def check_and_process(driver):
    wait = WebDriverWait(driver, 10)
    try:
        # Check if resultAnalysisList[1] exists
        result_analysis_list = driver.find_elements((By.ID, "resultAnalysisList1"))
        
        if len(result_analysis_list) > 1:  # Check if index 1 exists
            # Run the code below (Replace with your actual process)
            print("Processing as resultAnalysisList[1] exists...")
            # Your main processing code goes here
            
        else:
            print("resultAnalysisList[1] does not exist. Clicking 'Back'...")
            back_button = wait.until(EC.element_to_be_clickable((By.ID, "back")))
            back_button.click()
    
    except Exception as e:
        print(f"Error occurred: {e}")

# Main execution flow
if __name__ == "__main__":
    driver = get_driver()
    
    districts = ['02', '03', '04']  # Example 
    property_types = ['1', '2']  # Example
    
    for district in districts:
        for property_type in property_types:
            driver = select_district_and_property_type(driver, district, property_type)
            click_back_button(driver)
            # download_csv(driver)
    
    driver.quit()
