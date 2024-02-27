#!/usr/bin/env python3
#
#  Libraries and Modules
#
import configparser
import boto3
import time
import dynamoModules as dMods

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

econ_item = {
    "Country Name": "",
    "Currency": "",
    "GDP": []
}

config = configparser.ConfigParser()
config.read("S5-S3.conf")
aws_access_key_id = config['default']['aws_access_key_id']
aws_secret_access_key = config['default']['aws_secret_access_key']

try:
#
#  Establish an AWS session
#
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
#
#  Set up client and resources
#

    dynamoClient = session.client('dynamodb')

    print('Welcome to the world reporting program\nPlease give me a second to initialize data...')

    try:
        response = dynamoClient.describe_table(TableName='ksadikot_Econ_Data')
        print('Econ resource table exists')
    except dynamoClient.exceptions.ResourceNotFoundException:
        EconRes = dMods.create_table(dynamoClient,"Econ_Data")
        time.sleep(15)
        print('Creating economics resource table')
        econitems = dMods.parse_econ_data()   
        response = dMods.load_econ_records(dynamoClient, 'ksadikot_Econ_Data', econitems)
        print('Loading information into reosource table')
    
    try:
        response = dynamoClient.describe_table(TableName='ksadikot_Non_Econ_Data')
        print('Non econ resource table exists')
    except dynamoClient.exceptions.ResourceNotFoundException:
        nonEconRes = dMods.create_table(dynamoClient, "Non_Econ_Data")
        time.sleep(15)
        print('Creating non economics resource table')
        nonEconitems = dMods.parse_non_ecoc_data()
        response = dMods.load_non_econ_records(dynamoClient, 'ksadikot_Non_Econ_Data', nonEconitems)
        dMods.updateitem(dynamoClient)
        dMods.updateLanguageData(dynamoClient)
        print('Loading information into reosource table')




    print('Initialization successfully complete you may begin generating reports')

    userResponse = ''
    while(userResponse.lower() != 'quit'):
        print("please input which report you would like A or B. If none type quit to exit")
        userResponse = input()

        if userResponse == "a" or userResponse == "A": 
            print("Please a country: ")
            country = input()
            dMods.reportA(dynamoClient, country)
        elif userResponse == "b" or userResponse == "B":
            print('please enter a year: ')
            year = input()
            dMods.reportB(dynamoClient, year)
        

    #dMods.display_table(dynamoClient, 'ksadikot_Econ_Data')
    #dMods.display_table(dynamoClient, 'ksadikot_Non_Econ_Data')

    #dMods.delete_table(dynamoClient, 'ksadikot_Econ_Data')
    #dMods.delete_table(dynamoClient, 'ksadikot_Non_Econ_Data')
    #dMods.reportB(dynamoClient, '2008')
except Exception as e:
    print ( f'failed with exception: {e}' )