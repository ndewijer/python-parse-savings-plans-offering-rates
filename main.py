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


def pulldata_savingsplan():
    awssp = session.client('savingsplans')

    data = awssp.describe_savings_plans_offering_rates(
        products=['EC2'],
        savingsPlanTypes=['EC2Instance'],
        filters=awsfilter,
        maxResults=1000)
    resultArray = []
    nextToken = ""
    # for page in page_iterator:
    for result in data['searchResults']:

        resultArray.append(parsedata_savingsplan(result))

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

            resultArray.append(parsedata_savingsplan(result))

        if followup_data['nextToken'] != '':
            nextToken = followup_data['nextToken']
        else:
            nextToken = ''

    # json_object = json.dumps(resultArray, indent=4)
    # with open("results/data.json", "w") as outfile:
    #     outfile.write(json_object)

        df = pd.DataFrame(data['searchResults'])
        df.to_csv("results/searchResults.csv", index=False)

        return split_merge_savingsplan(resultArray)


def parsedata_savingsplan(result):
    valueDict = OrderedDict()

    duration = round(result['savingsPlanOffering']
                     ['durationSeconds'] / 60 / 60 / 24 / 365.25)

    valueDict['instanceType'] = next(
        item for item in result['properties'] if item["name"] == "instanceType")['value']
    valueDict['OS'] = next(item for item in result['properties']
                           if item["name"] == "productDescription")['value']
    valueDict['rate'] = result['rate']
    valueDict['unit'] = result['unit']
    valueDict['currency'] = result['savingsPlanOffering']['currency']
    valueDict['payplan'] = result['savingsPlanOffering']['planType']
    valueDict['paymentOption'] = result['savingsPlanOffering']['paymentOption']
    valueDict['OptionDuration'] = duration
    # valueDict['OptionDescription'] = result['savingsPlanOffering']['planDescription']

    return valueDict


def split_merge_savingsplan(data):
    oneyearArray = []
    threeyearArray = []

    for record in data:
        if record["OptionDuration"] == 1:
            record.pop("OptionDuration")
            record["rate/h 1y"] = record.pop("rate")
            oneyearArray.append(record)
        elif record["OptionDuration"] == 3:
            record.pop("OptionDuration")
            record["rate/h 3y"] = record.pop("rate")
            threeyearArray.append(record)

    oneyr_df = pd.DataFrame(oneyearArray)
    threeyr_df = pd.DataFrame(threeyearArray)

    result = pd.merge(oneyr_df, threeyr_df, on=[
                      "instanceType", "OS", "paymentOption", "payplan", "unit", "currency"], how="outer")
    result = result.drop_duplicates()
    return result


def add_ondemand():
    awsod = session.client('pricing', region_name="us-east-1")

    od_data = awsod.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "ServiceCode",
                "Value": "AmazonEC2"
            },
            {
                "Type": "TERM_MATCH",
                "Field": "operation",
                "Value": "RunInstances:0002"
            },
            {
                "Type": "TERM_MATCH",
                "Field": "regionCode",
                "Value": "eu-central-1"
            },
            {
                "Type": "TERM_MATCH",
                "Field": "tenancy",
                "Value": "shared"
            },
        ]
    )
    print(od_data)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sp_data = pulldata_savingsplan()
    # all_data = add_ondemand()

    df = pd.DataFrame(sp_data)
    df.to_csv("results/sp_data.csv", index=False)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
