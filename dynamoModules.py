#!/usr/bin/env python3
#
#  Libraries and Modules
#
import boto3
import csv
import json
from prettytable import PrettyTable
dynamoResource = boto3.resource('dynamodb')

# const objects
non_econ_items = []
non_econ_item = {
            "Country Name": "",
            "ISO3": "",
            "Area": "",
            "Capital": "",
            "Population": [],
            "Languages": [],
            "Official Name": "",
            "ISO2": ""
            }

econ_items = []
econ_item = {
    "Country Name": "",
    "Currency": "",
    "GDP": []
}

# created table in dynamodb with specified table name
def create_table( dynamo, tableName ):

    try:
        response = dynamo.create_table(

            TableName = f'ksadikot_{tableName}',
            AttributeDefinitions = [
                {
                    'AttributeName': 'Country Name',
                    'AttributeType': 'S'
                }
            ],
            KeySchema = [
                {
                    'AttributeName': 'Country Name',
                    'KeyType': 'HASH'
                }
            ],
            BillingMode='PAY_PER_REQUEST'

        )

        return response

    except Exception as e:
        print(f"Couldn't create table: ksadikot_{tableName} raised exception: {e}")

# deletes specified dynamodb table
def delete_table( dynamo, tableName ):

    try:
        response = dynamo.delete_table(TableName = f'ksadikot_{tableName}')

        return response

    except:
        print(f"Couldn't delete table: ksadikot_{tableName}")

# loads all items into non economic dynamodb table
def load_non_econ_records(dynamo, tableName, items):

    try:

        for item in items:
            population_json = json.dumps(item['Population'])
            
            dynamo.put_item(
                TableName = tableName,
                Item = {
                    'Country Name': {'S': item['Country Name']},
                    'ISO3': {'S': item['ISO3']},
                    'Area': {'S': item['Area']},
                    'Capital': {'S': item['Capital']},
                    'Population': {'S': population_json},
                    'Languages': {'SS': item['Languages']},
                    'Official Name': {'S': item['Official Name']},
                    'ISO2': {'S': item['ISO2']}
                }
            )

    except Exception as e:
        print(f"Couldn't load items into table: ksadikot_{tableName} raised excepetion: {e}")

# loads all items into economic dynamodb table
def load_econ_records(dynamo, tableName, items):

    try:
    
        for item in items:
            gdp_json = json.dumps(item['GDP'])

            dynamo.put_item(
                TableName = tableName,
                Item = {
                    'Country Name': {'S': item['Country Name']},
                    'Currency': {'S': item['Currency']},
                    'GDP': {'S': gdp_json}
                }
            )

    except Exception as e:
        print(f"Couldn't load items into table: ksadikot_{tableName} raised excepetion: {e}")

# goes through required files and parses required fields
def parse_non_ecoc_data():

    with open('shortlist_area.csv','r') as saf:
        reader = csv.DictReader(saf)
        for row in reader:
            temp_item = non_econ_item.copy()
            temp_item["Country Name"] = row["Country Name"]
            temp_item["ISO3"] = row["ISO3"]
            temp_item["Area"] = row["Area"]
            non_econ_items.append(temp_item)
    
    with open('shortlist_capitals.csv', 'r') as sac:
        reader = csv.DictReader(sac)
        for row in reader:
            for item in non_econ_items:
                if item['Country Name'] == row['Country Name']:
                    item['Capital'] = row['Capital']
                    break
    
    with open('shortlist_languages.csv', 'r') as sal:
        reader = csv.DictReader(sal)
        for row in reader:
            for item in non_econ_items:
                if item['Country Name'] == row['Country Name']:
                    item['Languages'] = row['Languages'].split('/')
                    break
    
    with open('un_shortlist.csv', 'r') as usf:
        
        reader = csv.DictReader(usf)
        for row in reader:
            for item in non_econ_items:
                if item['Country Name'] == row['Country Name']:
                    item['ISO2'] = row['ISO2']
                    item['Official Name'] = row['Official Name']
                    break

    with open('shortlist_curpop.csv', 'r') as scp:
        reader = csv.DictReader(scp)
        headers = reader.fieldnames    
        years = headers[2:]

        for row in reader:
            pop_items = []
            for year in years:
                population_item = {"Year": year, "Population": row[year]}
                pop_items.append(population_item)
            
            for item in non_econ_items:
                if item['Country Name'] == row['Country Name']:
                    item['Population'] = pop_items
    
    return non_econ_items
    
# goes through required files and parses required fields 
def parse_econ_data():
    
    with open('shortlist_curpop.csv') as scp:
        reader = csv.DictReader(scp)
        for row in reader:
            temp_item = econ_item.copy()
            temp_item["Country Name"] = row["Country Name"]
            temp_item["Currency"] = row["Currency"]
            econ_items.append(temp_item)
    
    with open('shortlist_gdppc.csv') as sgdp:
        reader = csv.DictReader(sgdp)
        rows = list(reader)
        headers = reader.fieldnames
        years = headers[1:]
        
        for row in rows: 
            gdp_items = []
            for year in years:
                gdp_item = {}
                if row[year] != "":
                    gdp_item = {'Year': year, 'GDP': row[year]}
                else:
                    gdp_item = {'Year': year, 'GDP': ""}
                gdp_items.append(gdp_item)
            
            for item in econ_items:
                if row['Country Name'] == item['Country Name']:
                    item['GDP'] = gdp_items
                
    
    return econ_items


# functionality to add single items into econ table
def add_econ_item(dynamo, item):

    gdp_json = json.dumps(item['GDP'])
    
    dynamo.put_item(
                TableName = 'ksadikot_Econ_Data',
                Item = {
                    'Country Name': {'S': item['Country Name']},
                    'Currency': {'S': item['Currency']},
                    'GDP': {'S': gdp_json}
                }
            )

# functionality to add item to non econ table
def add_non_econ_item(dynamo, item):

    population_json = json.dumps(item['Population'])
            
    dynamo.put_item(
        TableName = 'ksadikot_Non_Econ_data',
        Item = {
            'Country Name': {'S': item['Country Name']},
            'ISO3': {'S': item['ISO3']},
            'Area': {'S': item['Area']},
            'Capital': {'S': item['Capital']},
            'Population': {'S': population_json},
            'Languages': {'SS': item['Languages']},
            'Official Name': {'S': item['Official Name']},
            'ISO2': {'S': item['ISO2']}
        }
    )

# parses the missing_information json created
def parse_missing_information():

    with open('missing_information.json') as mif:
        data = json.load(mif)

    population_data = data['Population']
    temp_non_econ_list = []
    for country_data in population_data:
        temp_non_econ_item = non_econ_item.copy()
        temp_non_econ_item['Country Name'] = country_data['Country Name']
        population_item = {'Year': country_data['Year'], 'Population': country_data['popCount']}
        temp_non_econ_item['Population'].append(population_item)
        temp_non_econ_list.append(temp_non_econ_item)
    
    language_data = data['Languages']
    for country_data in language_data:
        temp_non_econ_item = non_econ_item.copy()
        temp_non_econ_item['Country Name'] = country_data['Country Name']
        temp_non_econ_item['Languages'].append(country_data['Languages']) 
        temp_non_econ_list.append(temp_non_econ_item)

    return temp_non_econ_list


# Function to update items that require updating from missing information.
# Functionality not complete as I am unable to figure out how to update the value within the object
def updateitem(dynamo):

    with open('missing_information.json') as mif:
        data = json.load(mif)

    population_data = data['Population']
    for item in population_data:
        
        response = dynamo.get_item(
            TableName='ksadikot_Non_Econ_Data', 
            Key={'Country Name': {'S': item['Country Name']}}
        )

        key_item = response['Item']
        population_data = json.loads(key_item['Population']['S'])

        for pop_item in population_data:
            if pop_item['Year'] == item['Year']:
                pop_item['Population'] = item['popCount']
        
        

        key_item['Population']['S'] = json.dumps(population_data)

        dynamo.put_item(
            TableName='ksadikot_Non_Econ_Data',
            Item = {
            'Country Name': key_item['Country Name'],
            'ISO3': key_item['ISO3'],
            'Area': key_item['Area'],
            'Capital': key_item['Capital'],
            'Population': key_item['Population'],
            'Languages': key_item['Languages'],
            'Official Name': key_item['Official Name'],
            'ISO2': key_item['ISO2']
            }
        )

def updateLanguageData(dynamo):

    with open('missing_information.json') as mif:
        data = json.load(mif)

    language_data = data['Languages']

    for item in language_data:

        response = dynamo.get_item(
            TableName='ksadikot_Non_Econ_Data', 
            Key={'Country Name': {'S': item['Country Name']}}
        )

        key_item = response['Item']

        langauge_list = key_item['Languages']['SS']
        langauge_list.append(item['Languages'])
        key_item['Languages']['SS'] = langauge_list

        dynamo.put_item(
            TableName='ksadikot_Non_Econ_Data',
            Item = {
            'Country Name': key_item['Country Name'],
            'ISO3': key_item['ISO3'],
            'Area': key_item['Area'],
            'Capital': key_item['Capital'],
            'Population': key_item['Population'],
            'Languages': key_item['Languages'],
            'Official Name': key_item['Official Name'],
            'ISO2': key_item['ISO2']
            }
        )



# Deletes single item in specified table
def delete_item(dynamo, table_name, Country):

    dynamo.delete_item(
        TableName = table_name,
        Key = {
            'Country Name': {'S': Country}
        }
    )

# Deletes specified table
def delete_table(dynamo, table_name):

    dynamo.delete_table(TableName=table_name)

# Displays specified table onto console
def display_table(dynamo, table_name):

    response = dynamo.scan(TableName=table_name)
    
    print(f'-----------------------{table_name}-----------------------------')
    if 'Items' in response:
        for item in response['Items']:
            print(item)
            print('------------------------------------------------------------------')

def reportA(dynamo, countryName):

    nonEconResponse = dynamo.get_item(
            TableName='ksadikot_Non_Econ_Data', 
            Key={'Country Name': {'S': countryName}}
        )
    
    econResponse = dynamo.get_item(
            TableName='ksadikot_Econ_Data', 
            Key={'Country Name': {'S': countryName}}
        )
    
    nonEconInfo = nonEconResponse['Item']
    econInfo = econResponse['Item']
    officialName = nonEconInfo['Official Name']['S']
    area = int(nonEconInfo['Area']['S'])
    languages = nonEconInfo['Languages']['SS']
    capitalCity = nonEconInfo['Capital']['S']
    population_data = json.loads(nonEconInfo['Population']['S'])
    gdp_data = json.loads(econInfo['GDP']['S'])
    currency = econInfo['Currency']['S']

    report = f"\n{countryName}\n[Official Name: {officialName}]\nArea: {area} sq km\nOfficial/National Languages: {languages}\nCapital City: {capitalCity}\n\nPOPULATION TABLE:"

    popTable = PrettyTable(['Year', 'Population', 'Population Density'])
    econTable = PrettyTable(['Year', 'GDPPC'])

    for pop_item in population_data:

        int_pop = int(pop_item['Population'])
        
        table_row = [pop_item['Year'], pop_item['Population'], int_pop/area]
        popTable.add_row(table_row)

    for gdp_item in gdp_data:
        table_row = [gdp_item['Year'], gdp_item['GDP']]
        econTable.add_row(table_row)

    print('---------------------------REPORT A---------------------------------------------')
    print(report)
    print(popTable)
    print(f"\nEconomics\nCurrency: {currency}\n")
    print(econTable)


def reportB(dynamo, year):


    nonEconResponse = dynamo.scan(TableName='ksadikot_Non_Econ_Data')
    econResponse = dynamo.scan(TableName='ksadikot_Econ_Data')

    nonEconItems = nonEconResponse['Items']
    econItems = econResponse['Items']

    rankings(nonEconItems, year)
    gdps(econItems)

    


def rankings(nonEconItems, year):
    popList = []
    areaList = []
    denList = []

    dataOb = {
        'Country Name': '',
        'Data': 0,
        'Rank': 0
    }

    for nonEconItem in nonEconItems:
        popOb = dataOb.copy()
        areaOb = dataOb.copy()
        denOb = dataOb.copy()

        popOb['Country Name'] = nonEconItem['Country Name']['S']
        areaOb['Country Name'] = nonEconItem['Country Name']['S']
        denOb['Country Name'] = nonEconItem['Country Name']['S']

        areaOb['Data'] = int(nonEconItem['Area']['S'])

        population_data = json.loads(nonEconItem['Population']['S'])

        for pop_item in population_data:
            if pop_item['Year'] == year:
                popOb['Data'] = int(pop_item['Population'])
        
        denOb['Data'] = popOb['Data']/areaOb['Data']

        popList.append(popOb)
        areaList.append(areaOb)
        denList.append(denOb)

    sortedPopList = sorted(popList, key=lambda x: x['Data'], reverse=True)
    sortedAreaList = sorted(areaList, key=lambda x: x['Data'], reverse=True)
    sortedDenList = sorted(denList, key=lambda x: x['Data'], reverse=True)

    popTable = PrettyTable(['Country Name', 'Population', 'Rank'])
    areaTable = PrettyTable(['Country Name', 'Area (sq km)', 'Rank'])
    denTable = PrettyTable(['Country Name', 'Density (people / sq km)', 'Rank'])

    for i, data in enumerate(sortedPopList):
        data['Rank'] = i + 1
        table_row = [data['Country Name'], data['Data'], data['Rank']]
        popTable.add_row(table_row)
    
    for i, data in enumerate(sortedAreaList):
        data['Rank'] = i + 1
        table_row = [data['Country Name'], data['Data'], data['Rank']]
        areaTable.add_row(table_row)

    for i, data in enumerate(sortedDenList):
        data['Rank'] = i + 1
        table_row = [data['Country Name'], data['Data'], data['Rank']]
        denTable.add_row(table_row)

    print('---------------------------REPORT B - GLOBAL REPORT---------------------------------------------')
    print(f"\nYear: {year}\nNumber of Countries: 40\nTable of Countries Ranked By Population (largest to smallest)")
    print(popTable)
    print("\nTable of Countries Ranked by Area (largest to smallest)")
    print(areaTable)
    print("\nTable of Countries Ranked by Density (largest to smallest)")
    print(denTable)


def gdps(econItems):

    table70s = PrettyTable(['Country Name', '1970', '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979'])
    table80s = PrettyTable(['Country Name', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989'])
    table90s = PrettyTable(['Country Name', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999'])
    table20s = PrettyTable(['Country Name', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009'])
    table21s = PrettyTable(['Country Name', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019'])

    

    for econItem in econItems:
        
        countryName = econItem['Country Name']['S']
        gdp_data = json.loads(econItem['GDP']['S'])
        list70s = []
        list80s = []
        list90s = []
        list20s = []
        list21s = []

        for data in gdp_data:
            if data['Year'].startswith('197'):
                list70s.append(data['GDP'])
            if data['Year'].startswith('198'):
                list80s.append(data['GDP'])
            if data['Year'].startswith('199'):
                list90s.append(data['GDP'])
            if data['Year'].startswith('200'):
                list20s.append(data['GDP'])
            if data['Year'].startswith('201'):
                list21s.append(data['GDP'])
        
        table70s.add_row([countryName, list70s[0], list70s[1], list70s[2], list70s[3], list70s[4], list70s[5], list70s[6], list70s[7], list70s[8] ,list70s[9]])
        table80s.add_row([countryName, list80s[0], list80s[1], list80s[2], list80s[3], list80s[4], list80s[5], list80s[6], list80s[7], list80s[8] ,list80s[9]])
        table90s.add_row([countryName, list90s[0], list90s[1], list90s[2], list90s[3], list90s[4], list90s[5], list90s[6], list90s[7], list90s[8] ,list90s[9]])
        table20s.add_row([countryName, list20s[0], list20s[1], list20s[2], list20s[3], list20s[4], list20s[5], list20s[6], list20s[7], list20s[8] ,list20s[9]])
        table21s.add_row([countryName, list70s[0], list21s[1], list21s[2], list21s[3], list21s[4], list21s[5], list21s[6], list21s[7], list21s[8] ,list21s[9]])
            
    print("\nGDP Per Capita for all Countries")
    print("\n1970's Table")
    print(table70s)
    print("\n1980's Table")
    print(table80s)
    print("\n1990's Table")
    print(table90s)
    print("\n2000's Table")
    print(table20s)
    print("\n2010's Table")
    print(table21s)