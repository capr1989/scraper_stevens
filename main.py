import json
import logging
from typing import List, Dict, Any
from bs4 import BeautifulSoup as BS
import aiohttp
import asyncio
import datetime

# Constants
STORE = 'stevens'
API_URL = "https://stevens.com.pa/products/index/productstock"
SCRAPE_BASE_URL = 'https://stevens.com.pa/dama/ropa-de-dama/'
CATEGORIES = ['abrigos', 'blazers-y-cardigans']  
MAX_PRODUCTS_PER_PAGE = 36  
SCRAPE_METADATA_FILE = 'scrape_metadata_stevens_github.json'
HEADERS = {
    'Cookie': 'PHPSESSID=e0bc426ce8502f15c6f8628eb6e22d7d; X-Magento-Vary=491329fa615c9137110864c861cd31afceb6266aa332a5486dbba4302e281f99; form_key=K1aa7SHhFwhDokN5; private_content_version=cdf258e374174fb8f6a23aad8beb09d2',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

API_HEADERS = {
    'accept': '*/*',
    'accept-language': 'es-PA,es-419;q=0.9,es;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-messages=; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; PHPSESSID=e0bc426ce8502f15c6f8628eb6e22d7d; form_key=K1aa7SHhFwhDokN5; mage-cache-sessid=true; X-Magento-Vary=491329fa615c9137110864c861cd31afceb6266aa332a5486dbba4302e281f99; private_content_version=7bd0a7e92ea9670ed54612fdee465e5f; section_data_ids={%22cart%22:1733233347}; PHPSESSID=e0bc426ce8502f15c6f8628eb6e22d7d; X-Magento-Vary=491329fa615c9137110864c861cd31afceb6266aa332a5486dbba4302e281f99; form_key=K1aa7SHhFwhDokN5; private_content_version=dc31f3e4b22e9eb43095703034068842',
    'origin': 'https://stevens.com.pa',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-newrelic-id': 'VgMOVV5QCRAEUlVSAwcCX1E=',
    'x-requested-with': 'XMLHttpRequest'
}

PRODUCT_PAGE_ATAG = 'a.product.photo.product-item-photo'
DATA_TAG = "script[type='text/x-magento-init']"
SCHEMA_TAG = "script[type='application/ld+json']"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def get_responses(url: str, session: aiohttp.ClientSession) -> str:
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status != 200:
                logging.error(f"Failed to get response from {url}, Status Code: {response.status}")
                return ""
            return await response.text()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return ""

def get_soup(response: str) -> BS:
    return BS(response, 'html.parser')

def format_payload(sku: str, size: Dict[str, Any], color: Dict[str, Any]) -> str:
    return f"sku={sku}&options%5B277%5D={color['id']}&options%5B617%5D={size['id']}"

async def post_response(url: str, session: aiohttp.ClientSession, payload: str) -> Dict[str, Any]:
    try:
        async with session.post(url, data=payload, headers=API_HEADERS) as response:
            if response.status != 200:
                logging.error(f"Failed to post to {url}, Status Code: {response.status}")
                return {}
            response_text = await response.text()
            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                logging.error(f"Failed to parse JSON response for payload: {payload}")
                return {}
    except Exception as e:
        logging.error(f"Error posting to {url} with payload {payload}: {e}")
        return {}

async def scrape_category(session: aiohttp.ClientSession, category: str, max_products_per_page: int) -> List[Dict[str, Any]]:
    all_products_data = []
    page = 1
    
    while True:
        url = f"{SCRAPE_BASE_URL}{category}.html?p={page}"
        logging.info(f"Fetching page {page} of category '{category}': {url}")
        
        # Fetch the category page
        response = await get_responses(url, session)
        if not response:
            logging.warning(f"No response received for {url}. Stopping pagination for category '{category}'.")
            break
        
        soup = get_soup(response)
        product_tags = soup.select(PRODUCT_PAGE_ATAG)
        num_products = len(product_tags)
        
        logging.info(f"Found {num_products} products on page {page} of category '{category}'.")
        
        # If no products found, stop pagination
        if num_products == 0:
            logging.info(f"No products found on page {page} of category '{category}'. Ending pagination.")
            break
        
        # Extract product hrefs from the current page
        hrefs = [tag.get('href') for tag in product_tags if tag.get('href')]
        
        # fetch and process a single product page
        async def fetch_and_process(href: str, idx: int):
            product_response = await get_responses(href, session)
            product_soup = get_soup(product_response)
            return product_soup
        
        # Fetch all product pages concurrently
        product_soups = await asyncio.gather(*[fetch_and_process(href, idx) for idx, href in enumerate(hrefs, 1)])
        
        for idx, product_soup in enumerate(product_soups, 1):
            if not product_soup:
                logging.warning(f"Skipping empty product page at index {idx} on page {page} of category '{category}'.")
                continue

            data = product_soup.select(DATA_TAG)
            schema = product_soup.select(SCHEMA_TAG)
            
            # Parse schema JSON
            try:
                schema_json = json.loads(schema[0].text) if schema else {}
            except json.JSONDecodeError as e:
                logging.error(f"JSON decoding error for schema in product page {idx} on page {page}: {e}")
                schema_json = {}
            
            sku = schema_json.get('sku')
            
            json_data = {}
            for d in data:
                if "data-role" in d.text:
                    try:
                        json_data = json.loads(d.text)
                        break  
                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decoding error in data tag for product page {idx} on page {page}: {e}")
            
            if json_data:
                swatch = json_data.get("[data-role=swatch-options]", {})
                magento_swatch_data = swatch.get("Magento_Swatches/js/swatch-renderer", {}).get("jsonConfig", {})
                if not magento_swatch_data:
                    logging.warning(f"No swatch data found for product page {idx} on page {page}.")
                    continue
                prices = magento_swatch_data.get('optionPrices', {})
                productId = magento_swatch_data.get('productId', None)
                images = magento_swatch_data.get('images', [])
                color_attributes = magento_swatch_data.get('attributes', {}).get('277', {}).get('options', [])
                size_attributes = magento_swatch_data.get('attributes', {}).get('617', {}).get('options', [])

                product_data = {
                    "schema": schema_json,
                    "productId": productId,
                    "images": images,
                    "sku": sku,
                    "prices": prices,
                    'category': category,
                    'store': STORE,
                    'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "variants": []
                }

                # Create variant combinations
                variant_combinations = [
                    (color, size) for color in color_attributes for size in size_attributes
                ]

                # Prepare tasks with payloads and their corresponding color and size
                tasks = []
                for color, size in variant_combinations:
                    formatted_payload = format_payload(sku, size, color)
                    tasks.append((formatted_payload, color, size))
                
                # Create post tasks
                post_tasks = [post_response(API_URL, session, pl) for pl, _, _ in tasks]
                
                # Gather responses
                responses = await asyncio.gather(*post_tasks)
                
                # Map each response to its corresponding variant
                for (payload, color, size), response in zip(tasks, responses):
                    if response:
                        variant_id = response.get('id') 
                        
                        variant_data = {
                            "variant_id": variant_id,
                            "color": color.get('label'),
                            "color_id": color.get('id'),
                            "size": size.get('label'),
                            "size_id": size.get('id'),
                            "response": response  
                        }
                        product_data['variants'].append(variant_data)
                    else:
                        logging.warning(f"No response received for payload: {payload}")
                
                all_products_data.append(product_data)
        
        # Check whether to continue to the next page based on the number of products found
        if num_products < max_products_per_page:
            logging.info(f"Page {page} of category '{category}' has fewer than {max_products_per_page} products. Ending pagination.")
            break
        
        page += 1  
    return all_products_data

async def main(categories: List[str]) -> List[Dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        all_categories_data = await asyncio.gather(*[scrape_category(session, category, MAX_PRODUCTS_PER_PAGE) for category in categories])
        logging.info("All categories have been scraped.")
        return all_categories_data

if __name__ == "__main__":
    # Run the main function with the list of categories
    scraped_data = asyncio.run(main(CATEGORIES))
    
    with open(SCRAPE_METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, ensure_ascii=False, indent=4)
    
    logging.info(f"Scraped data saved to {SCRAPE_METADATA_FILE}.")
