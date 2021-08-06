#importing the libraries necessary to request and transform indicator data
import sys
import pandas as pd
import requests
import json
import numpy as np
import os
import zipfile
import json # Needed for handling JSON
import csv # Needed for writing to csv
import urllib
import glob

# pyodbc may not be included in the python standard library and would require a pip install
# if this is the case, install it with pip via "pip install pyodbc" within your environment
# For additional support, access the module's docs here: https://pypi.org/project/pyodbc/
import pyodbc

'''
DxHub project: Early action for Early Action (Evaluating Food Security Risk)

This code fetches data from credible sources (currently just World Bank and FAO) of specific indicators
that provide information about the levels of food insecurity for specific groups.

Currently this code focuses on Latin America, but more countries can be included by adding them to the "countries"
list below.
'''


'''
Can't get pyodbc imported into glue

possible resources:
https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html
https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html
'''


'''
In the prototype, a microsoft sql server was used through amazon RDS with pyodbc. If other database engines are used, then
ensure their drivers are accessible with pyodbc before simply changing the values below.
'''

DB_DRIVER = "SQL Server"
DB_PORT = "5439"
DB_SERVER = 'food-insecurity-dashboard-data.cwkwbva5fmjk.us-west-2.rds.amazonaws.com'
DB_DATABASE="Dashboard"
DB_USER = "dashboard_user"
DB_PASSWORD = "Z4>z9%+za),Vq4!J"


# List of countries to be included in the dashboard
countries = ['Argentina', 'Brazil', 'Mexico', 'El Salvador', 'Haiti', 'Colombia', 
             'Peru', 'Venezuela', 'Guatemala', 'Honduras', 'Nicaragua', 'Bolivia', 
             'Chile', 'Ecuador', 'Paraguay', 'Uruguay', 'Belize', 'Costa Rica', 
             'Dominica', 'Jamaica', 'Panama', 'Antigua and Barbuda', 'Bahamas',
             'Barbados', 'Cuba', 'Grenada', 'Guyana', 'St. Kitts and Nevis', 
             'St. Lucia', 'St. Vincent and the Grenadines', 'Suriname', 
             'Trinidad and Tobago', 'Dominican Republic']
    

def main():
    # Names of csv file names for each source, as long as these end in .csv the names shouldn't matter
    csv_paths = {
        "World Bank": "WB_Final.csv",
        "FAO": "FAO_Final.csv"
    }
    print("Connecting to database...")
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.fast_executemany = True
    print("Database connected")
    
    # fetch and format data
    print("Fetching data and transforming it into one csv per source")
    WorldBank(csv_paths["World Bank"])
    FAO(csv_paths["FAO"])
    
    # Assuming that all CSV files have the same schema (["Country", "Year", "Indicator", "Value"])
    # This will 
    aggregate_and_upload_data(list(csv_paths.values()), connection, cursor)
    

def connect_to_db():
    '''
    Uses pyodbc to connect to a database. It checks if the database, DB_DATABASE, has been created. If not, then
    it creates it before returning the connection.
    '''
    # Checks to see if initial database connection is possible and if the database needs to be created or not
    test_connection = pyodbc.connect(f'DRIVER={DB_DRIVER};PORT={DB_PORT};SERVER={DB_SERVER};UID={DB_USER};PWD={DB_PASSWORD};autocommit=False')
    test_cursor = test_connection.cursor()
    test_resp = test_cursor.execute("SELECT name FROM master.sys.databases;").fetchall()
    databases = [name[0] for name in resp]
    if DB_DATABASE not in databases:
        test_cursor.execute(f"CREATE DATABASE {DB_DATABASE};")
        test_connection.commit()
    test_cursor.close()
    test_connection.close()
    
    return pyodbc.connect(f'DRIVER={DRIVER};PORT={PORT};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};autocommit=False')

    
def aggregate_and_upload_data(csv_paths, connection, cursor):
    '''
    Reads in the csv files, combines the data into one dataframe, and then uploads data to datbase
    in different formats
    '''
    def rel_diff(a, b):
        # calculates the "relative percent difference" as explained here: 
        # https://stats.stackexchange.com/questions/86708/how-to-calculate-relative-error-when-the-true-value-is-zero
        if a==0 or b==0: 
            return 0
        return 2 * ((b - a) / (abs(a) + abs(b)))

    
    def check_rankings(rankings):
        '''
        if all rankings are the same, then the relative ranking is not useful and we don't want this to skewing
        the results when searching for the most impactful indicators. 
        ex: Since we are measuring relative differences, the first indicator's year will always be made up of the same values.
        If all rankings are the same, then all rankings will become 1
        '''
        if len(rankings) <= 1:
            return rankings
        replacement_rankings = []
        previous_rank = rankings[0]
        for rank in rankings[1:]:
            if rank != previous_rank:
                return rankings
            else:
                replacement_rankings.append(1)
        replacement_rankings.append(1)  # accounts for the append missed by not iterating over the first value
        return replacement_rankings
    
    
    def get_indicator_category(indicator):
        '''
        Hard coded maps to map indicator names to the category they belong to.
        Currently the category is the source of the data
        '''
        indicator_map = {
            "World Bank" : ['WB Official exchange rate (LCU per US$, period average)',
                            'WB Food exports (% of merchandise exports)',
                            'WB Food imports (% of merchandise imports)',
                            'WB Agriculture, forestry, and fishing, value added (% of GDP)',
                            'WB Agriculture, forestry, and fishing, value added (annual % growth)',
                            'WB Personal remittances, received (% of GDP)',
                            'WB Annual freshwater withdrawals, agriculture (% of total freshwater withdrawal)',
                            'WB Adjusted net national income per capita (annual % growth)',
                            'WB Gini Index (WB estimate)',
                            'WB Income share held by highest 10%',
                            'WB Income share held by highest 20%',
                            'WB Income share held by lowest 10%',
                            'WB Income share held by lowest 20%',
                            'WB Employment in Agriculture',
                            'WB Employment in agriculture - female (% of female employment) (modeled ILO estimate)',
                            'WB Agricultural irrigated land (% of total agricultural land)',
                            'WB Adequacy of social safety net programs (% of total welfare of beneficiary households)',
                            'WB Adequacy of unemployment benefits and ALMP (% of total welfare of beneficiary households)',
                            'WB Prevalence of moderate or severe food insecurity in the population (%)',
                            'WB Prevalence of severe food insecurity in the population (%)',
                            'WB Poverty headcount ratio at national poverty lines (% of population)',
                            'WB Multidimensional Poverty Headcount Ratio, children (% of population ages 0-17)',
                            'WB Multidimensional poverty headcount ratio (% of total population)',
                            'WB Multidimensional poverty headcount ratio, female (% of female population)'],
            "FAO" : ['FAO Crop and livestock exports - quantity (tonnes)',
                    'FAO Crop and livestock exports - value (1000 US$)',
                    'FAO Crop and livestock imports - quantity (tonnes)',
                    'FAO Crop and livestock imports - value (1000 US$)',
                    'FAO Per capita food supply variability (kcal/cap/day)',
                    'FAO Percentage of population using at least basic drinking water services (percent)',
                    'FAO Prevalence of anemia among women of reproductive age (15-49 years)',
                    'FAO Prevalence of low birthweight (percent)',
                    'FAO Prevalence of obesity in the adult population (18 years and older)',
                    'FAO Per capita food production variability (constant 2004-2006 thousand int$ per capita)',
                    'FAO Average dietary energy supply adequacy (percent) (3-year average)',
                    'FAO Average protein supply (g/cap/day) (3-year average)',
                    'FAO Cereal import dependency ratio (percent) (3-year average)',
                    'FAO Share of dietary energy supply derived from cereals, roots and tubers (kcal/cap/day) (3-year average)',
                    'FAO Percentage of children under 5 years affected by wasting (percent)',
                    'FAO Percentage of children under 5 years of age who are overweight (percent)',
                    'FAO Percentage of children under 5 years of age who are stunted (percent)',
                    'FAO Employment-to-population ratio, rural areas (%)',
                    'FAO Employment-to-population ratio, rural areas, female (%)',
                    'FAO Share of food consumption in total income (Engel ratio) (mean)']
        }
        for category_indicators in indicator_map.items():
            if indicator in category_indicators[1]:
                return category_indicators[0]
        print(indicator)
        return "None"
    
    
    def fix_null_values(values):
        '''
        For rows of values, if any of the values are "nan", this function changes them to None.
        '''
        new_values = []
        for row in values:
            new_row = []
            for val in row:
                if str(val) == "nan":
                    new_row.append(None)
                else:
                    new_row.append(val)
            new_values.append(new_row)
        return new_values
    

    def upload_table(connection, cursor, df, table_name, schema):
        '''
        Takes in the pyodbc connection and cursor, a dataframe full of values, the table name
        in the database to allocate those values, and the database schema to define datatypes and
        relations for inserting rows from the dataframe
        
        schema is in the form of [(column name, data type), ...] and should match the order of the
        columns in the dataframe
        '''
        sql_schema = ""
        sql_cols = ""
        params_to_sub = ""
        for name_type in schema[:-1]:
            sql_schema += f"{name_type[0]} {name_type[1]},"
            sql_cols += f"{name_type[0]}, "
            params_to_sub += "?,"
        sql_schema += f"{schema[-1][0]} {schema[-1][1]}"
        sql_cols += schema[-1][0]
        params_to_sub += "?"

        cursor.execute(
            f'''
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}({sql_schema});
            ''')

        # Assumes dataframe column order is the same as schema
        df_values = [tuple(x) for x in fix_null_values(df.values)]
        stmt = f'''INSERT INTO {table_name} ({sql_cols}) VALUES ({params_to_sub})'''

        cursor.executemany(stmt, df_values)
        connection.commit()
        
        
        
    ### Joins then uploads the data into "Final" table
    final_df_cols = ["Country", "Year", "Indicator", "Value"]
    final_df = pd.DataFrame(columns=final_df_cols)
    for path in csv_paths:
        temp_df = pd.read_csv(path)
        final_df = final_df.append(temp_df)

    final_df = final_df.sort_values(["Country", "Year", "Indicator"])
    final_df = final_df.reset_index(drop=True)

    final_df_table_name = "Final"
    final_df_schema = [("Country", "NCHAR(100)"),
                       ("Year", "INT"), 
                       ("Indicator", "NCHAR(500)"),
                       ("Value", "FLOAT")]

    upload_table(connection, cursor, final_df, final_df_table_name, final_df_schema)
    
    ### Pivots then uploads the data in "Final_Pivoted" table
    final_pivoted_df = final_df.pivot(index=["Country", "Year"], columns="Indicator", values="Value").reset_index()
    final_pivoted_df_table_name = "Final_Pivoted"

    # First two columns are country and year, the rest are indicators with their values
    final_pivoted_df_schema = [("Country", "NCHAR(100)"),
                               ("Year", "INT")]

    for col in final_pivoted_df.columns[2:]:
        final_pivoted_df_schema.append(("\"" + col + "\"", "FLOAT"))

    upload_table(connection, cursor, final_pivoted_df, final_pivoted_df_table_name, final_pivoted_df_schema)

    ### Calculates the relative difference for each value across consecutive years.
    
    relative_df = final_df.sort_values(["Country", "Indicator", "Year"]).reset_index(drop=True)

    previous_country = ""
    previous_indicator = ""
    relative_values = []

    for index, row in relative_df.iterrows():
        current_country = row["Country"]
        current_indicator = row["Indicator"]
        current_value = row["Value"]
        if previous_country != current_country or previous_indicator != current_indicator:
            # New indicator or country row
            relative_values.append(0)
        else:
            # Same indicator and country
            relative_values.append(rel_diff(previous_value, current_value))
        previous_country = current_country
        previous_indicator = current_indicator
        previous_value = current_value

    relative_df['Difference'] = relative_values

    
    ### Calculates the magnitude of change for each value and then ranks indicators based on magnitude of change per year
    relative_df["Abs Diff"] = abs(relative_df["Difference"])
    relative_df = relative_df.sort_values(["Indicator", "Year", "Abs Diff"], ascending=[True, True, False]).reset_index(drop=True)
    
    total_rankings = []
    indicator_values = []
    previous_indicator = ""
    previous_year = 0
    for index, row in relative_df.iterrows():
        current_indicator = row["Indicator"]
        current_year = row["Year"]
        current_value = row["Abs Diff"]
        if current_indicator != previous_indicator or current_year != previous_year:
            # figure out ranking here
            indicator_rankings = pd.Series(indicator_values, dtype="float64").rank().tolist()
            total_rankings.extend(check_rankings(indicator_rankings))
            indicator_values = [current_value]
        else:
            indicator_values.append(current_value)
        previous_indicator = current_indicator
        previous_year = current_year
    indicator_rankings = pd.Series(indicator_values, dtype="float64").rank().tolist()
    total_rankings.extend(check_rankings(indicator_rankings))
    relative_df["Rank"] = total_rankings
    
    
    ### Adds a categorical variable to each value. Here, the category is the source
    categories = []
    for index, row in relative_df.iterrows():
        indicator = row["Indicator"]
        categories.append(get_indicator_category(indicator))

    full_indicator_data_df = relative_df
    full_indicator_data_df["Category"] = categories

    full_indicator_data_df_table_name = "Full_Indicator_Data"
    full_indicator_data_df_schema = [("Country", "NCHAR(100)"),
                                     ("Year", "INT"), 
                                     ("Indicator", "NCHAR(500)"),
                                     ("Value", "FLOAT"),
                                     ("Difference", "FLOAT"),
                                     ("\"Abs Diff\"", "FLOAT"),
                                     ("Rank", "FLOAT"),
                                     ("Category", "NCHAR(100)")]

    upload_table(connection, cursor, full_indicator_data_df, full_indicator_data_df_table_name, full_indicator_data_df_schema)
    
    ### Creates a table of external links for indicators that are missing data
    ### These are all the sources that are not yet included in the database, but are relevant to measuring
    ###    global food insecurity
    external_sources = {
        "FAO Price Warnings" : "http://www.fao.org/giews/food-prices/price-warnings/en/",
        "WFP Food Price Alert" : "https://dataviz.vam.wfp.org/economic_explorer/price-forecasts-alerts?adm0=108",
        "WFP Hunger Map" : "https://hungermap.wfp.org/",
        "IPC Population" : "http://www.ipcinfo.org/ipc-country-analysis/population-tracking-tool/en/",
        "IMF CPI - All Items" : "https://data.imf.org/regular.aspx?key=61015894",
        "ND-GAIN Vulnerability Index" : "https://gain.nd.edu/our-work/country-index/rankings/",
        "WB Logistics Performance Index Score" : "https://lpi.worldbank.org/international/aggregated-ranking",
        "IMF Unemployment Rate" : "https://www.imf.org/external/datamapper/LUR@WEO/CZE",
        "IMF GDP Per Capita" : "https://www.imf.org/external/datamapper/PPPPC@WEO/OEMDC/ADVEC/WEOWORLD"
    }

    external_cols = ["Indicator/Source", "URL"]
    external_df = pd.DataFrame(columns=external_cols)

    for i, indicator_url in enumerate(external_sources.items()):
        external_df.loc[i] = [indicator_url[0], indicator_url[1]]

    # external_df.to_excel("External_Sources.xlsx", index=False)


    external_df_table_name = "External_Sources"
    external_df_schema = [("Source", "NCHAR(500)"),
                          ("URL", "NCHAR(500)")]

    upload_table(connection, cursor, external_df, external_df_table_name, external_df_schema)
    print("Uploads complete")


class WorldBank:
    '''
    World Bank Data Injestion 
    Most Code written by Braden Michelson, built off the work of Charlie Taylor and Nathaniel Cinnamon
    
    Imports indicator data from World Bank API. A csv is created for each indicator and then aggregated into one csv file.
    '''
    
    # List of all possible column names with associated indicator codes (country and year included in each csv)
    headers = ['Country',
              'Country Code',
              'Year', 
              'Multidimensional poverty headcount ratio (% of total population)',
              'Employment in Agriculture',
              'Gini Index (WB estimate)',
              'Multidimensional Poverty Headcount Ratio, children (% of population ages 0-17)',
              'Multidimensional poverty headcount ratio, female (% of female population)',
              'Prevalence of severe food insecurity in the population (%)',
              'Prevalence of moderate or severe food insecurity in the population (%)',
              'Adequacy of social safety net programs (% of total welfare of beneficiary households)',
              'Income share held by highest 10%',
              'Income share held by highest 20%',
              'Income share held by lowest 10%',
              'Income share held by lowest 20%',
              'Personal remittances, received (% of GDP)',
              'Poverty headcount ratio at national poverty lines (% of population)',
              'Agriculture, forestry, and fishing, value added (% of GDP)',
              'Agricultural irrigated land (% of total agricultural land)',
              'Adequacy of unemployment benefits and ALMP (% of total welfare of beneficiary households)',
              'Adjusted net national income per capita (annual % growth)',
              'Employment in agriculture - female (% of female employment) (modeled ILO estimate)',
              'Agriculture, forestry, and fishing, value added (annual % growth)',
              'Food exports (% of merchandise exports)',
              'Food imports (% of merchandise imports)',
              'Official exchange rate (LCU per US$, period average)',
              'Annual freshwater withdrawals, agriculture (% of total freshwater withdrawal)']

    # List of all indicator codes for API calls
    codes = ['SI.POV.MDIM',
             'SL.AGR.EMPL.ZS',
             'SI.POV.GINI',
             'SI.POV.MDIM.17',
             'SI.POV.MDIM.FE',
             'SN.ITK.SVFI.ZS',
             'SN.ITK.MSFI.ZS',
             'per_sa_allsa.adq_pop_tot',
             'SI.DST.10TH.10',
             'SI.DST.05TH.20',
             'SI.DST.FRST.10',
             'SI.DST.FRST.20',
             'BX.TRF.PWKR.DT.GD.ZS',
             'SI.POV.NAHC',
             'NV.AGR.TOTL.ZS',
             'AG.LND.IRIG.AG.ZS',
             'per_lm_alllm.adq_pop_tot',
             'NY.ADJ.NNTY.PC.KD.ZG',
             'SL.AGR.EMPL.FE.ZS',
             'NV.AGR.TOTL.KD.ZG',
             'TX.VAL.FOOD.ZS.UN',
             'TM.VAL.FOOD.ZS.UN',
             'PA.NUS.FCRF',
             'ER.H2O.FWAG.ZS']

    
    def __init__(self, final_name, delete_extra_csv_files=True, print_missing_countries=False):
        # If user doesn't want the extra csv files deleted then they must initialize object with False as input
        self.missing_countries = {}
        self.scattered_csv_names = []
        self.final_aggregated_csv_name = final_name
        print("Fetching World Bank data")
        self.fetchData()
        print("Transforming World Bank data")
        self.transformData()
        if delete_extra_csv_files:
            for filename in self.scattered_csv_names:
                os.remove(filename)
        if print_missing_countries:
            printMissingCountries()
        
        
    def wbRenameCountryNames(self, df):
        '''
        Reassigns region names to be consistent, only includes Latin American countries at the moment
        NOTE: "Country" must be the name for the country column
        '''
        name_map = [
            ('Venezuela, RB','Venezuela'),
            ('Bahamas, The','Bahamas')      
        ]
        for names in name_map:
            df.Country = df.Country.replace(names[0], names[1])

            
    def checkMissingCountries(self, df, countries):
        '''
        Checks if any target countries are missing from the dataframe
        If any, it returns a list of them. Otherwise it returns an empty list
        '''
        missing = []
        csv_countries = df.Country.unique()
        for country in countries:
            if country not in csv_countries:
                missing.append(country)
        return missing
    
    
    def printMissingCountries(self):
        # Prints out the missing target countries for each indicator dataset
        print("\n")
        for kv in missing_countries.items():
            print(f"{kv[0]} is missing data for {len(kv[1])} countries: \n{kv[1]}\n")
        
    
    def fetchData(self):
        '''
        Below, we are looping through each indicator following in the list of codes above, 
        and pluggin each indicator into the url request.
        
        Need to set a max value for how many results per page
        to do this, realize we have 259 countries and (current date - 1960) 
        years worth of data. Set a max results per page > (total years)*(total countries)
        '''
        max_results = 20000
        self.scattered_csv_names = []
        for i in range(len(WorldBank.codes)):  
            csv_file_name = WorldBank.codes[i]+"_yearly.csv"
            self.scattered_csv_names.append(csv_file_name)
            call = open(csv_file_name, 'w') # creating file name, this is for the gini index variable
            write = csv.writer(call, dialect = 'excel') # where file will write to
            # Writing the first row (country, year, variable = gini index)
            write.writerow([WorldBank.headers[0],WorldBank.headers[1],WorldBank.headers[2],WorldBank.headers[i+3]])

            url = 'http://api.worldbank.org/v2/country/all/indicator/'+str(WorldBank.codes[i])+'?per_page='+str(max_results)+'&format=json'
            req = requests.get(url)
            page = req.json()
            for entry in range(len(page[1])):
                country = page[1][entry]['country']['value']
                countrycode = page[1][entry]['countryiso3code'] # pulling country iso3 code
                value = page[1][entry]['value'] # pulling variable of interest value 
                year = page[1][entry]['date'] # pulling year at which value occured
                row = [country, countrycode, int(year), value] # creating row of (country, year, value)
                write.writerow(row) # writing row to our csv
            call.close()


    def transformData(self):
        # Data filtering, aggregation, and transformation into csv
         
        self.missing_countries = {}
        cols = ["Country", "Year", "Indicator", "Value"]
        WB_df = pd.DataFrame(columns=cols)

        for csv_file in self.scattered_csv_names:
            # Expected input column format: ["Country", "Country Code", "Year" "{Indicator Name With Unit}"]
            # Desired output column format: ["Country", "Year" "Indicator", "Value"]
            new_WB_df = pd.read_csv(csv_file)
            indicator_name = new_WB_df.columns[3]
            new_WB_df = new_WB_df.drop(columns=["Country Code"])
            new_WB_df = new_WB_df.rename(columns={indicator_name: 'Value'})
            new_WB_df["Indicator"] = "WB " + indicator_name
            new_cols = ["Country", "Year", "Indicator", "Value"]
            new_WB_df = new_WB_df[new_cols]
            new_WB_df = new_WB_df.dropna(subset=["Value"])
            self.wbRenameCountryNames(new_WB_df)
            self.missing_countries[csv_file] = self.checkMissingCountries(new_WB_df, countries)
            new_WB_df = new_WB_df[new_WB_df.Country.isin(countries)]
            WB_df = WB_df.append(new_WB_df)

        WB_df.to_csv(self.final_aggregated_csv_name, index=False)
        

class FAO:
    '''
    FAO Data Injestion 
    Code written by Braden Michelson, built off the work of Charlie Taylor and Nathaniel Cinnamon
    Imports indicator data from FAO API. A dataframe is created for each source / api call and then aggregated into one csv file.
    '''

    def __init__(self, final_name, get_core_items=True):
        self.get_core_items = get_core_items
        self.final_aggregated_csv_name = final_name
        print("FAO Source 1:")
        FAO_df = self.source1()
        print("FAO Source 2:")
        FAO_df = FAO_df.append(self.source2())
        print("FAO Source 3:")
        FAO_df = FAO_df.append(self.source3())
        print("FAO Source 4:")
        FAO_df = FAO_df.append(self.source4())
        print("FAO Source 5:")
        FAO_df = FAO_df.append(self.source5())
        print("FAO Source 6:")
        FAO_df = FAO_df.append(self.source6())

        # Rearrange and rename columns
        new_cols = ["Area", "Year", "Item", "Value"]
        FAO_df = FAO_df[new_cols]
        # Rename indicator column
        FAO_df = FAO_df.rename(columns={'Area': 'Country'})
        FAO_df = FAO_df.rename(columns={'Item': 'Indicator'})

        FAO_df.to_csv(final_name, index=False)

    def getFile(self, url, file_name):
        '''
        Uses a binary stream to download a file from an api call
        '''
        r = requests.get(url, stream=True)
        handle = open(file_name, "wb")
        for chunk in r.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                handle.write(chunk)
        handle.close()


    def getCsvFromOnlineZip(self, url, zip_path, csv_name):
        '''
        url: url of zip folder to download
        zip_path: what to name zip file in local file system
        csv_name: name of csv in zip file and how it is saved in local file system

        downloads the zip file, extracts and saves the relevant csv, and then removes the zip file
        '''
        self.getFile(url, zip_path)

        # Extracts the relevant csv file from the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            with open(csv_name, 'wb') as bulk_csv:
                bulk_csv.write(zip_file.read(csv_name))

        # Deletes the zip file, leaving just the relevant csv
        # NOTE: If the section above runs into an error, the zip file won't be removed
        os.remove(zip_path)


    def faoRenameAreaNames(self, df):
        # Reassigns region names to be consistent, only include Latin American countries at the moment
        # NOTE: "Area" must be the name for the country column
        name_map = [
            ('Venezuela (Bolivarian Republic of)','Venezuela'),
            ('Bolivia (Plurinational State of)','Bolivia'),
            ('Saint Kitts and Nevis','St. Kitts and Nevis'),
            ('Saint Lucia','St. Lucia'),
            ('Saint Vincent and the Grenadines','St. Vincent and the Grenadines')
        ]
        for names in name_map:
            df.Area = df.Area.replace(names[0], names[1])


    def source2(self):
        '''
        Source 2: FAO - Crops and livestock products - http://www.fao.org/faostat/en/#data/TP

        Yields the indicators -> found by:
        FAO Crop and livestock exports - value -> Aggregating columns from bulk dataset
        FAO Crop and livestock imports - value -> Aggregating columns from bulk dataset
        (DROPPED due to inconsistent units) FAO Crop and livestock exports - quantity -> Aggregating columns from bulk dataset
        (DROPPED due to inconsistent units) FAO Crop and livestock imports - quantity -> Aggregating columns from bulk dataset
        '''

        # Data fetching
        # url to get bulk csv zip file. zip_path is how I save the zip file locally
        url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_CropsLivestock_E_All_Data.zip'
        zip_path = 'Trade_CropsLivestock_E_All_Data.zip'

        # Name of bulk csv file in zip file and how I save it locally
        csv_name = 'Trade_Crops_Livestock_E_All_Data_NOFLAG.csv'

        self.getCsvFromOnlineZip(url, zip_path, csv_name)


        # Data filtering
        '''
        The Crops and livestock products csv was allegedly not encoded in utf-8, had to use latin1
        This suggests that multi-byte encoding won't work for this dataset which means
             that complex characters might have been misinterpreted as multiple simple ones.
        I could not find any data that looks encoded incorrectly and do not know why complex
             characters would even be in this simple dataset. This is simply a warning.
        '''
        FAO_cl_df = pd.read_csv(csv_name, encoding="latin1")

        # Filters out Import and export quantities
        indicators = ["Export Value", "Import Value"]
        FAO_cl_df = FAO_cl_df[FAO_cl_df.Element.isin(indicators)]
        
        '''
        Let the element column include the unit, so we can drop the unit column
        This assumes that if a rows have the same element, then they have the same unit
            or else the data aggregation below would not group together correctly
            FAO_cl_df.Element.unique() can be used to ensure the success of this command
            There should only be four options
        '''
        FAO_cl_df["Element"] = FAO_cl_df["Element"] + " (" + FAO_cl_df["Unit"] + ")"

        # Drop unnecessary columns
        FAO_cl_df = FAO_cl_df.drop(columns=["Area Code", "Item Code", "Element Code", "Unit"])

        # Restructure to have Area, Item, and Year define each row
        FAO_cl_df = FAO_cl_df.melt(id_vars=["Area", "Item", "Element"], var_name="Year", value_name="Value")

        # Dropping rows with null values
        FAO_cl_df = FAO_cl_df.dropna(subset=["Value"])

        # Reformatting the year. ex: Turns Y2000 into 2000
        # Assumptions: Format is in Yxxxx, where x could be any number
        FAO_cl_df.Year = FAO_cl_df.Year.str.strip("Y")

        
        # Data aggregation and more filtering
        # Aggregate data, grouping by area, element, and year

        FAO_cl_df = FAO_cl_df.groupby(['Area', 'Element', 'Year']).agg({'Value': 'sum'})
        FAO_cl_df.reset_index(inplace=True)

        # Warning: hard coded strings
        #     This depends on the (now) four element options for each item in the initial csv
        #         and the string appending for the element column above.
        # Need to rename elements to reflect the aggregation
        FAO_cl_df.Element = FAO_cl_df.Element.replace('Export Value (1000 US$)', 'FAO Crop and livestock exports - value (1000 US$)')
        FAO_cl_df.Element = FAO_cl_df.Element.replace('Import Value (1000 US$)', 'FAO Crop and livestock imports - value (1000 US$)')

        FAO_cl_df = FAO_cl_df.rename(columns={'Element': 'Item'})

        # Reassigning region names to be consistent
        self.faoRenameAreaNames(FAO_cl_df)

        # Filter dataframe to only include target countries
        # NOTE: this must take place after region names are reassigned
        FAO_cl_df = FAO_cl_df[FAO_cl_df.Area.isin(countries)]

        # final: 
        return FAO_cl_df

    
    def source3(self):
        '''
        Source 3: FAO Data Search For "cereal import dependency ratio" - http://www.fao.org/faostat/en/#search/cereal%20import%20dependency%20ratio
        FAO Cereal import dependency ratio
        NOTE: this is a 3-year average ratio that is then represented in the dataframe as one year (ex: 2001-2003 -> 2004)
        '''
        
        # Data fetching
        api_call = 'http://fenixservices.fao.org/faostat/api/v1/en/data/FS?item=21035&output_type=csv'
        csv_name = "FAO_Cereal_import_dependency_ratio.csv"
        self.getFile(api_call, csv_name)


        # Data filtering
        FAO_cidr_df = pd.read_csv(csv_name)

        # Altering year interval to be one year (ex: 2001-2003 -> 2004)
        year_list = FAO_cidr_df.Year.tolist()
        for i in range(len(year_list)):
            year_interval = year_list[i].split("-")
            if len(year_interval) != 2:
                # print(f"Format error: {year_interval}")
                continue
            end_year = int(year_interval[1].strip()) + 1
            year_list[i] = str(end_year)
        FAO_cidr_df.Year = year_list

        # Drop unnecessary columns
        FAO_cidr_df = FAO_cidr_df.drop(columns=["Domain Code", "Domain", "Area Code", "Item Code", "Element Code", "Element", 
                                                "Year Code", "Unit", "Flag", "Flag Description", "Note"])

        # Reassigning region names to be consistent
        self.faoRenameAreaNames(FAO_cidr_df)

        # Filter dataframe to only include target countries
        # NOTE: this must take place after region names are reassigned
        FAO_cidr_df = FAO_cidr_df[FAO_cidr_df.Area.isin(countries)]

        # Additional formatting
        FAO_cidr_df.Item = "FAO " + FAO_cidr_df.Item

        # final: 
        return FAO_cidr_df


    def source4(self):
        '''   
        Source 4: FAO - Indicators from Household Surveys - http://www.fao.org/faostat/en/#data/OE
        FAO Share of food consumption in total income
        --
        Specific api call made with the options:
            Survey: All
            Breakdown Variable: "Country-Level (List)"
            Breakdown by sex of the household head: Total
            Indicator: Share of food consumption in total income (Engel ratio)
            Measure: Mean (other options: median, std dev, number of observations)
        NOTE: this dataset lacks data for most countries and years
        --
        '''
        # Data fetching
        api_call = 'http://fenixservices.fao.org/faostat/api/v1/en/data/HS?        survey=32005%2C522006%2C1620002001%2C162005%2C1920032004%2C1152004%2C1152009%2C392009%2C1072002%2C591997%2C8119981999%2C892006%2C9319992000%2C972004%2C1032007%2C11420052006%2C1202008%2C1262002%2C13020042005%2C1332001%2C1382004%2C1382006%2C1382008%2C14420022003%2C14919951996%2C15820072008%2C16520052006%2C1662008%2C1681996%2C16919971998%2C1712003%2C1462006%2C3819992000%2C2062009%2C2082007%2C1762001%2C2172006%2C22620022003%2C22620052006%2C23620042005%2C23719921993%2C2372006%2C25120022003&breakdownvar=2307%3E&breakdownsex=20000&indicator=6067&measure=6076&show_codes=true&show_unit=true&show_flags=true&null_values=false&output_type=csv'
        csv_name = "FAO_Share_of_food_consumption_in_total_income.csv"
        self.getFile(api_call, csv_name)


        # Data filtering
        FAO_sofciti_df = pd.read_csv(csv_name)
        
        '''
        Altering Survey column to expand into the year and area columns
        (ex: 'Ghana - 1998-1999' -> 'Ghana', '1999')
        Assumptions: format is in "{country} - {year}" or "{country} - {year}-{year}"
        Notice that in this case I decide to use the end of the interval instead of the following year
        This is to conform to the dashboard format and because these surveys represent two years or one per row,
           This avoids errors with the case where there could be entries such as:
              'Ghana - 1998-1999' and 'Ghana - 2000'
           In the other datasets, an indicator represented an interval or a single year, and we cannot
              make that assumption here
        '''
        year_list = []
        country_list = []
        survey_list = FAO_sofciti_df.Survey.tolist()
        for survey in survey_list:
            survey_split = survey.split("-")
            year = int(survey_split[-1].strip())
            year_list.append(year)

            # This is to account for countries that might contain "-" in their name
            country = survey_split[0].strip()
            for entry in survey_split[1:]:
                try:
                    int(entry.strip())
                    break
                except ValueError:
                    country += "-" + entry.strip()
            country_list.append(country)

        FAO_sofciti_df["Area"] = country_list
        FAO_sofciti_df["Year"] = year_list

        # Drop unnecessary columns
        # Assumptions: One measure value (mean in this case), Unit is in indicator
        FAO_sofciti_df = FAO_sofciti_df.drop(columns= ["Domain Code", "Domain", "Survey Code", "Survey", "Breakdown Variable Code",
                                                "Breakdown Variable", "Breadown by Sex of the Household Head Code",
                                                 "Breadown by Sex of the Household Head", "Indicator Code", 
                                                 "Measure Code", "Measure", "Unit", "Flag", "Flag Description"])

        self.faoRenameAreaNames(FAO_sofciti_df)
        FAO_sofciti_df = FAO_sofciti_df[FAO_sofciti_df.Area.isin(countries)]

        FAO_sofciti_df = FAO_sofciti_df.rename(columns={'Indicator': 'Item'})

        FAO_sofciti_df.Item = "FAO " + FAO_sofciti_df.Item + " (mean)"

        # final: 
        return FAO_sofciti_df


    def source5(self):
        '''
        Source 5: FAO - Employment Indicators - http://www.fao.org/faostat/en/#data/HS
        Yields the indicators -> labeled in csv as:
        FAO Employment-to-population ratio, rural areas -> 'Employment-to-population ratio, rural areas'
        FAO Employment-to-population ratio, rural areas - female -> 'Employment-to-population ratio, rural areas, female'
        NOTE: this dataset lacks data for most countries
        '''

        indicators = ['Employment-to-population ratio, rural areas',
                      'Employment-to-population ratio, rural areas, female']


        # Data fetching
        # url to get bulk csv zip file. zip_path is how I save the zip file locally
        url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/Employment_Indicators_E_All_Data.zip'
        zip_path = 'Employment_Indicators_E_All_Data.zip'

        # Name of bulk csv file in zip file and how I save it locally
        csv_name = 'Employment_Indicators_E_All_Data_NOFLAG.csv'

        self.getCsvFromOnlineZip(url, zip_path, csv_name)    

        # Data filtering
        # Warning: latin1 encoding, not utf-8
        FAO_ei_df = pd.read_csv(csv_name, encoding="latin1")

        # Rename indicator column
        FAO_ei_df = FAO_ei_df.rename(columns={'Indicator': 'Item'})

        self.faoRenameAreaNames(FAO_ei_df)

        # Filter dataframe to only include target indicators
        FAO_ei_df = FAO_ei_df[FAO_ei_df.Item.isin(indicators)]

        # Filter dataframe to only include target countries
        FAO_ei_df = FAO_ei_df[FAO_ei_df.Area.isin(countries)]

        # Append unit to indicator
        FAO_ei_df.Item = FAO_ei_df.Item + " (" + FAO_ei_df.Unit + ")"

        # Drop unnecessary columns
        FAO_ei_df = FAO_ei_df.drop(columns=["Area Code", "Source Code", "Source", "Indicator Code", "Unit"])

        # Restructure to have Area, Item, and Year define each row
        FAO_ei_df = FAO_ei_df.melt(id_vars=["Area", "Item"], var_name="Year", value_name="Value")

        # Dropping rows with null values
        FAO_ei_df = FAO_ei_df.dropna(subset=["Value"])

        # Reformatting the year. ex: Turns Y2000 into 2000
        # Assumptions: Format is in Yxxxx, where x could be any number
        FAO_ei_df.Year = FAO_ei_df.Year.str.strip("Y")

        # Additional formatting
        FAO_ei_df.Item = "FAO " + FAO_ei_df.Item

        # final: 
        return  FAO_ei_df

    
    def source6(self):
        '''
        Source 6: FAO - Agriculture Total - http://www.fao.org/faostat/en/#data/GT
        FAO Total Agricultural Emissions (in CO2 equivalents)
        --
        Specific api call made with the options:
            Countries: All
            Elements: Emissions (CO2eq)
            Items Aggregated: IPCC Agriculture + (Total)
            Years: All (also offers a 'year projections' option and that might be useful)
        --
        '''
        # Data fetching

        api_call = 'http://fenixservices.fao.org/faostat/api/v1/en/data/GT?area=2%2C3%2C4%2C5%2C6%2C7%2C258%2C8%2C9%2C1%2C22%2C10%2C11%2C52%2C12%2C13%2C16%2C14%2C57%2C255%2C15%2C23%2C53%2C17%2C18%2C19%2C80%2C20%2C21%2C239%2C26%2C27%2C233%2C29%2C35%2C115%2C32%2C33%2C36%2C37%2C39%2C259%2C40%2C351%2C96%2C128%2C214%2C41%2C44%2C45%2C46%2C47%2C48%2C98%2C49%2C50%2C167%2C51%2C107%2C116%2C250%2C54%2C72%2C55%2C56%2C58%2C59%2C60%2C61%2C178%2C63%2C209%2C238%2C62%2C65%2C64%2C66%2C67%2C68%2C69%2C70%2C74%2C75%2C73%2C79%2C81%2C82%2C84%2C85%2C86%2C87%2C88%2C89%2C90%2C175%2C91%2C93%2C94%2C95%2C97%2C99%2C100%2C101%2C102%2C103%2C104%2C264%2C105%2C106%2C109%2C110%2C112%2C108%2C114%2C83%2C118%2C113%2C120%2C119%2C121%2C122%2C123%2C124%2C125%2C126%2C256%2C129%2C130%2C131%2C132%2C133%2C134%2C127%2C135%2C136%2C137%2C270%2C138%2C145%2C140%2C141%2C273%2C142%2C143%2C144%2C28%2C147%2C148%2C149%2C150%2C151%2C153%2C156%2C157%2C158%2C159%2C160%2C161%2C154%2C163%2C162%2C221%2C164%2C165%2C180%2C299%2C166%2C168%2C169%2C170%2C171%2C172%2C173%2C174%2C177%2C179%2C117%2C146%2C183%2C185%2C184%2C182%2C187%2C188%2C189%2C190%2C191%2C244%2C192%2C193%2C194%2C195%2C272%2C186%2C196%2C197%2C200%2C199%2C198%2C25%2C201%2C202%2C277%2C203%2C38%2C276%2C206%2C207%2C260%2C210%2C211%2C212%2C208%2C216%2C176%2C217%2C218%2C219%2C220%2C222%2C223%2C213%2C224%2C227%2C228%2C226%2C230%2C225%2C229%2C215%2C240%2C231%2C234%2C235%2C155%2C236%2C237%2C243%2C205%2C249%2C248%2C251%2C181&area_cs=FAO&element=7231&item=1711&year=1961%2C1962%2C1963%2C1964%2C1965%2C1966%2C1967%2C1968%2C1969%2C1970%2C1971%2C1972%2C1973%2C1974%2C1975%2C1976%2C1977%2C1978%2C1979%2C1980%2C1981%2C1982%2C1983%2C1984%2C1985%2C1986%2C1987%2C1988%2C1989%2C1990%2C1991%2C1992%2C1993%2C1994%2C1995%2C1996%2C1997%2C1998%2C1999%2C2000%2C2001%2C2002%2C2003%2C2004%2C2005%2C2006%2C2007%2C2008%2C2009%2C2010%2C2011%2C2012%2C2013%2C2014%2C2015%2C2016%2C2017%2C2018&show_codes=true&show_unit=true&show_flags=true&null_values=false&output_type=csv'
        
        csv_name = "FAO_Total_Agricultural_Emissions_in_CO2_equivalents.csv"
        self.getFile(api_call, csv_name)

        
        # Data filtering
        FAO_CO2_df = pd.read_csv(csv_name)

        # Rename indicator column
        FAO_CO2_df = FAO_CO2_df.rename(columns={'Item': 'Source'})
        FAO_CO2_df = FAO_CO2_df.rename(columns={'Element': 'Item'})

        self.faoRenameAreaNames(FAO_CO2_df)

        # Filter dataframe to only include target countries
        FAO_CO2_df = FAO_CO2_df[FAO_CO2_df.Area.isin(countries)]

        # Append unit to indicator, hard coded because using FAO_CO2_df["Unit"] was giving me a type error for some reason
        FAO_CO2_df.Item = FAO_CO2_df.Item + " (" + FAO_CO2_df.Unit + ")"

        # Drop unnecessary columns
        FAO_CO2_df = FAO_CO2_df.drop(columns=["Domain Code", "Domain", "Area Code (FAO)", "Element Code", "Item Code",
                                             "Year Code", "Unit", "Flag", "Flag Description", "Note", "Source"])

        return FAO_CO2_df
    

if __name__=="__main__":
    main()

