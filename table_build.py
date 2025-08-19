from bs4 import BeautifulSoup
import requests
import pandas as pd
import pycountry
import json
import warnings
import duckdb
import logging

logging.basicConfig(level=logging.INFO, force=True)

warnings.filterwarnings('ignore')  # Suppress all warning

with open("nuffic_mapping.json", "r") as f:
    nuffic_mapping = json.load(f)
    
countries = [
    "afghanistan",
    "albania",
    "algeria",
    "argentina",
    "aruba",
    "australia",
    "austria",
    "bangladesh",
    "belgium-flemish-community",
    "belgium-french-community",
    "bosnia-and-herzegovina",
    "brazil",
    "bulgaria",
    "cameroon",
    "canada",
    "chile",
    "china",
    "colombia",
    "costa-rica",
    "croatia",
    "curacao-st-maarten-and-the-bes-islands",
    "cyprus",
    "czechia",
    "denmark",
    "ecuador",
    "egypt",
    "eritrea",
    "estonia",
    "ethiopia",
    "european-baccalaureate",
    "finland",
    "france",
    "georgia",
    "germany",
    "ghana",
    "greece",
    "hong-kong",
    "hungary",
    "iceland",
    "india",
    "indonesia",
    "international-baccalaureate",
    "iran",
    "iraq",
    "ireland",
    "israel",
    "italy",
    "japan",
    "jordan",
    "kazakhstan",
    "kenya",
    "latvia",
    "lebanon",
    "lithuania",
    "luxembourg",
    "malaysia",
    "mexico",
    "moldova",
    "morocco",
    "nepal",
    "new-zealand",
    "nigeria",
    "north-macedonia",
    "norway",
    "pakistan",
    "peru",
    "philippines",
    "poland",
    "portugal",
    "romania",
    "russia",
    "rwanda",
    "saudi-arabia",
    "serbia",
    "singapore",
    "slovakia",
    "slovenia",
    "south-africa",
    "south-korea",
    "spain",
    "sri-lanka",
    "sudan",
    "surinam",
    "sweden",
    "switzerland",
    "syria",
    "taiwan",
    "tanzania",
    "thailand",
    "tunisia",
    "turkey",
    "the-netherlands",
    "uganda",
    "ukraine",
    "united-kingdom-england-wales-and-northern-ireland",
    "united-kingdom-scotland",
    "united-states",
    "venezuela",
    "vietnam",
    "yemen",
    "zimbabwe"
]

def lookup_alpha_3(country_name):
    """Returns an alpha 3 code for a given country name else returns an error message.

    Args:
        country_name (string): The country name to look up.

    Returns:
        An alpha 3 code from the pycountry library
    """
    try:
        alpha_3 = pycountry.countries.get(name=country_name).alpha_3
        country_name = pycountry.countries.get(name=country_name).name
        return alpha_3, country_name
    except Exception as e:
        unknown = "UNK"
        country_name = "Unknown"
        logging.warning(f"Failed to find country: {country_name}")
        return unknown, country_name

def create_df_from_rows(con, rows):
    
    # load grade mappings
    with open('australia_mapping.json', 'r') as aus_mapping_file:
        aus_mapping = json.load(aus_mapping_file)
        
    # load grade mappings for uk
    with open('united_kingdom_mapping.json', 'r') as uk_mapping_file:
        uk_mapping = json.load(uk_mapping_file)
    
    # make a dataframe 
    df = pd.DataFrame(rows[1:], columns=rows[0])
    source_country_iso3_code, country_name = lookup_alpha_3(con)

    # add to the dataframe
    df["source_country_iso3_code"] = source_country_iso3_code
    df["country_name"] = country_name
    
    df = df.rename(columns={
        "Comparable to": "NLD_equivalent",
        "Diploma": "grade_type"
        }
    )
    
    if "Comparable" in df.columns:
        df = df.rename(columns={
            "Comparable": "NLD_equivalent",
        }
    )
        
    # Australia equivalency
    df['AUS_equivalent'] = df['NLD_equivalent'].apply(lambda x: aus_mapping.get(x, "No equivalency"))
    
    # United Kingdom equivalency
    df['GBR_equivalent'] = df['NLD_equivalent'].apply(lambda x: uk_mapping.get(x, "No equivalency"))
    
    df = df[[
        "source_country_iso3_code",
        "country_name",
        "grade_type",
        "NLD_equivalent",
        "AUS_equivalent",
        "GBR_equivalent"
    ]]
    
    return df

def get_table_from_nuffic(countries):
    
    with open("md_token.json", 'r') as token_file:
        token = json.load(token_file)
    
    md_token = token["token"]
    duck_con = duckdb.connect(f'md:equivalency_database?motherduck_token={md_token}')
    duck_con.sql("use equivalency_database;")
    
    dfs = []
    for con in countries:
        
        try:
            mapped_con = nuffic_mapping.get(con)
            # if mapped_con:
            #     con = mapped_con
            url = f"https://www.nuffic.nl/en/education-systems/{con.lower()}/level-of-diplomas"
            page = requests.get(url, verify=False)
            soup = BeautifulSoup(page.content, "html.parser")
            table = soup.find("table")    
            if table:
                rows = []
                for tr in table.find_all("tr"):
                    cells = tr.find_all(["td", "th"])
                    row = [cell.get_text(strip=True) for cell in cells]
                    if row:
                        rows.append(row)

                # make a dataframe
                if mapped_con:
                    con = mapped_con
                df = create_df_from_rows(con=con, rows=rows)
                dfs.append(df)
                logging.info(f"Created -> {con}")
            else:
                url = f"https://www.nuffic.nl/en/education-systems/{con.lower()}"
                page = requests.get(url, verify=False)
                soup = BeautifulSoup(page.content, "html.parser")
                
                # find the table
                table = soup.find("table")
                
                rows = []
                for tr in table.find_all("tr"):
                    cells = tr.find_all(["td", "th"])
                    row = [cell.get_text(strip=True) for cell in cells]
                    if row:
                        rows.append(row)
                        
                # make a dataframe
                if mapped_con:
                    con = mapped_con
                df = create_df_from_rows(con=con, rows=rows)
                dfs.append(df)
                logging.info(f"Created -> {con}")
        except Exception as e:
            logging.warning(f"Failed on -> {con}")
            pass
    
    df = pd.concat(dfs)
    duck_con.sql("create or replace table equivalencies as select * from df;")
    logging.info(f"Created equivalencies table")
    duck_con.close()
    logging.info(f"Process complete")

dfs = get_table_from_nuffic(countries)