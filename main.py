import boto3
import json
import csv
from collections import OrderedDict
import pandas as pd

awsregion = 'eu-central-1'
awsfilter = [
    {'name': 'region', 'values': [awsregion]},
    {'name': 'productDescription', 'values': ['Linux/UNIX', 'Windows']},
    {'name': 'instanceFamily', 'values': ['t3',
                                          't2',
                                          't3a',
                                          'c5',
                                          'c5a',
                                          'r5',
                                          'r5a',
                                          'r6',
                                          'r6a',
                                          'm5',
                                          'm5a']},
    {'name': 'tenancy', 'values': ['shared']}
]

session = boto3.Session(profile_name='')
awssp = session.client('savingsplans')


def pulldata():

    data = awssp.describe_savings_plans_offering_rates(
        products=['EC2'],
        savingsPlanTypes=['EC2Instance'],
        filters=awsfilter,
        maxResults=1000)
    resultArray = []
    nextToken = ""
    # for page in page_iterator:
    for result in data['searchResults']:

        resultArray.append(parsedata(result))

    if data['nextToken'] != '':
        nextToken = data['nextToken']

    while nextToken != '':
        followup_data = awssp.describe_savings_plans_offering_rates(
            products=['EC2'],
            savingsPlanTypes=['EC2Instance'],
            filters=awsfilter,
            maxResults=1000,
            nextToken=nextToken
        )
        for result in followup_data['searchResults']:

            resultArray.append(parsedata(result))

        if followup_data['nextToken'] != '':
            nextToken = followup_data['nextToken']
        else:
            nextToken = ''

    # json_object = json.dumps(resultArray, indent=4)
    # with open("results/data.json", "w") as outfile:
    #     outfile.write(json_object)

        df = pd.DataFrame(data['searchResults'])
        df.to_csv("results/searchResults.csv", index=False)

        return resultArray


def parsedata(result):
    valueDict = OrderedDict()

    duration = round(result['savingsPlanOffering']
                     ['durationSeconds'] / 60 / 60 / 24 / 365.25)

    valueDict['instanceType'] = next(
        item for item in result['properties'] if item["name"] == "instanceType")['value']
    valueDict['OS'] = next(item for item in result['properties']
                           if item["name"] == "productDescription")['value']
    valueDict['ondemand_rate'] = result['rate']
    valueDict['ondemand_unit'] = result['unit']
    valueDict['currency'] = result['savingsPlanOffering']['currency']
    valueDict['payplan'] = result['savingsPlanOffering']['planType']
    valueDict['paymentOption'] = result['savingsPlanOffering']['paymentOption']
    valueDict['OptionDuration {0} year'.format(duration)] = duration
    # valueDict['OptionDescription'] = result['savingsPlanOffering']['planDescription']

    return valueDict


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    data = pulldata()

    df = pd.DataFrame(data)
    df.to_csv("results/data.csv", index=False)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
