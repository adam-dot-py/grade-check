from fastapi import FastAPI
import json
import duckdb

from pydantic import BaseModel

class CountryInfo(BaseModel):
    source_iso_country_code: str
    country_name: str
    grade_type: str
    equivalent_grade: str

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

# this call gets the closest matching grade
@app.get("/grades/{source_country_iso}/{destination_country_iso}", response_model=list[CountryInfo])
async def get_grade(source_country_iso: str, grade: str, destination_country_iso: str):
    
    with open("md_token.json", 'r') as token_file:
        token = json.load(token_file)
    
    md_token = token["token"]
    duck_con = duckdb.connect(f'md:equivalency_database?motherduck_token={md_token}')
    duck_con.sql("use equivalency_database;")
    
    destinations = {
        "NLD": "NLD_equivalent",
        "GBR": "GBR_equivalent",
        "AUS": "AUS_equivalent"
    }
    
    destination = destinations.get(destination_country_iso.upper())
    # Query data
    query = f"""
        SELECT 
          source_country_iso3_code, 
          country_name, 
          grade_type,
          {destination} as equivalent_grade
        FROM equivalencies
        WHERE 
          source_country_iso3_code = '{source_country_iso.upper()}'
          AND grade_type ILIKE '%{grade.lower()}%'
    """
    
    result = duck_con.sql(query).fetchall()
    duck_con.close()
    
    # api return
    if result:
        return [ {
            "source_iso_country_code": row[0],
            "country_name": row[1],
            "grade_type": row[2],
            "equivalent_grade": row[3]
        }  for row in result]
    else:
        return "No information found for the given parameters."

# this call lists all the grades for the source country in the database and returns their destination equivalents
@app.get("/list-grades/{source_country_iso}/{destination_country_iso}")
def list_grades(source_country_iso: str, destination_country_iso: str):
    with open("md_token.json", 'r') as token_file:
        token = json.load(token_file)
    
    md_token = token["token"]
    duck_con = duckdb.connect(f'md:equivalency_database?motherduck_token={md_token}')
    duck_con.sql("use equivalency_database;")
    
    destinations = {
        "NLD": "NLD_equivalent",
        "GBR": "GBR_equivalent",
        "AUS": "AUS_equivalent"
    }
    
    destination = destinations.get(destination_country_iso.upper())
    
    query = f"""
    SELECT 
      source_country_iso3_code, 
      country_name, 
      grade_type,
      {destination} as equivalent_grade
    FROM equivalencies
    WHERE 
      source_country_iso3_code = '{source_country_iso.upper()}'
    """
    
    result = duck_con.sql(query).fetchall()
    duck_con.close()
    
    # api return
    if result:
        return [ {
            "source_iso_country_code": row[0],
            "country_name": row[1],
            "grade_type": row[2],
            "equivalent_grade": row[3]
        }  for row in result]
    else:
        return "No information found for the given parameters."