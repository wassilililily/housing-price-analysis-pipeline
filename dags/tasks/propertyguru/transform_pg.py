from airflow.decorators import task
import re
from datetime import datetime

@task
def transform_propertyguru(listings):

    def parse_details(details_list):
        parsed = {}
        for item in details_list:
            if "bedrooms" in item:
                match = re.search(r'\d+', item)
                if match:
                    parsed["bedrooms"] = int(match.group())
            elif "bathrooms" in item:
                match = re.search(r'\d+', item)
                if match:
                    parsed["bathrooms"] = int(match.group())
            elif "sqft" in item:
                match = re.search(r'[\d,.]+', item)
                if match:
                    parsed["sqft"] = float(match.group().replace(",", ""))
            elif "psf" in item:
                match = re.search(r'[\d,.]+', item)
                if match:
                    parsed["price_psf"] = float(match.group().replace(",", ""))
            elif "Listed on" in item:
                date_match = re.search(r'Listed on (.+?) \(', item)
                if date_match:
                    try:
                        parsed["listed_date"] = datetime.strptime(date_match.group(1), "%b %d, %Y")
                    except ValueError:
                        pass  # Ignore if date format is invalid
        return parsed
    
    def parse_info(info_list):
        parsed = {
            "built": None,
            "tenure": None,
            "property_type": None
        }
        
        for item in info_list:
            if "Built" in item or "New Project" in item: # Built includes New Project that will be completed in the future
                match = re.search(r'\d{4}', item)
                if match:
                    parsed["built"] = int(match.group())
            elif "Leasehold" in item:
                match = re.search(r'\d+', item)
                if match:
                    years = int(match.group())
                    parsed["tenure"] = 999 if years >= 999 else years
            elif "Freehold" in item:
                parsed["tenure"] = 999
            elif "Unknown tenure" in item:
                parsed["tenure"] = None
            else:
                parsed["property_type"] = item
        return parsed
    
    def transform(listings):
        transformed_all = []
        for item in listings:
            # Check if any field in the listing is None, if so, skip this listing
            if any(value is None for value in item.values()):
                continue
            transformed = {
                "id": item["id"],
                "title": item["title"],
                "address": item["address"],
                "price": int(re.search(r'[\d,]+', item["price"]).group().replace(",", "")),
                "agent_description": item["agent_description"],
            }

            transformed.update(parse_details(item["details"]))
            transformed.update(parse_info(item["info"]))
            transformed_all.append(transformed)

        return transformed_all
    
    return transform(listings)