# --Add company name by cleansing customername column and  contact name by cleansing addressinformationline column
# --use the customername column and pass it to ewg website and pull all the subsidy crops and fetch the details

import numpy as np
import pandas as pd
import re

import numpy as np
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time


# Read the CSV file
df = pd.read_csv("openfive.csv")

# Print total rows and columns
total_rows, total_columns = df.shape
print("Total rows:", total_rows)
print("Total columns:", total_columns)


# #--------------------------------first column---------------------------------------------------#
# df['Modified_Customername'] = df['Customername'].apply(lambda x: '' if str(x).isdigit() else re.sub(r'[^a-zA-Z0-9\s]', ' ', str(x)))

# # Replace empty strings with "nan" in the Modified_Customername column
# df.loc[df['Modified_Customername'].str.strip() == '', 'Modified_Customername'] = np.nan

# #--------------------------------second column---------------------------------------------------#
# # Remove special characters and replace them with spaces in AddressInformationLine column
# df['Contactname'] = df['AddressInformationLine'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', ' ', str(x)))

# # Remove records containing digits or '#' symbols from AddressInformationLine column
# df['Contactname'] = df['Contactname'].apply(lambda x: '' if re.match(r'.*\d+.*|#', str(x)) else x)

# # Replace empty strings with "nan" in the Contactname column
# df.loc[df['Contactname'].str.strip() == '', 'Contactname'] = np.nan



#----------------------------------------ewgwebsite column---------------------------------------------#
def get_ewg_website(customer_name, state):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    search_url = f"https://farm.ewg.org/addrsearch.php?zip=&searchstring={customer_name.replace(' ', '+')}&stab={state.replace(' ', '+')}&z=See+Recipients"
    print("Search URL:", search_url)
    
    while True:
        search_response = requests.get(search_url, headers=headers)
        print(search_response)

        if search_response.status_code == 200:
            search_soup = BeautifulSoup(search_response.content, 'html.parser')

            # Find all links
            links = search_soup.find_all('a', href=True)

            # Search for the link that leads to a farm page
            farm_link = None
            for link in links:
                if link.get('href') and link.get('href').startswith("persondetail.php"):
                    farm_link = link
                    break
            
            if farm_link:
                farm_url = "https://farm.ewg.org/" + farm_link['href']
                #print("Farm URL:", farm_url)
                
                # Extract custnumber from the farm_url if it's available
                custnumber_match = re.search(r'custnumber=(.*)', farm_url)
                if custnumber_match:
                    custnumber = custnumber_match.group(1)
                    ewg_url = f"https://farm.ewg.org/persondetail.php?custnumber={custnumber}"
                    print("EWG URL:", ewg_url)
                    return ewg_url
                else:
                    return "No custnumber found"
            else:
                return "No link found"
        elif search_response.status_code == 429:
            print("Too many requests. Retrying after a delay...")
            time.sleep(10)  # Wait for 60 seconds before retrying
        else:
            print("Error:", search_response.status_code)
            return None

df['ewgwebsite'] = df.apply(lambda row: get_ewg_website(row['Customername'], row['State']), axis=1)

#----------------------------------------save new csv ---------------------------------------------------#
df.to_csv("closefive.csv", index=False, na_rep='nan')

#--------------------------after add column counting-------------------------------------------#
total_rows_after, total_columns_after = df.shape
print("Total rows after:", total_rows_after)
print("Total columns after:", total_columns_after)


