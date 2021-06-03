import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import time

from google.cloud import bigquery
from google.cloud import storage
from web3 import Web3

GET_ABI_ENDPOINT = 'https://api.etherscan.io/api?module=contract&action=getabi&address='
TX_LIST_ENDPOINT = 'https://api.etherscan.io/api?module=account&action=txlist&address='
DENOMINATOR = 1e18

def upload_staked_sold():

    w3 = Web3(Web3.HTTPProvider(os.environ.get("INFURA_PROVIDER_MAIN")))

    def get_abi(addr):
        response = requests.get('%s%s&apikey=%s'%(GET_ABI_ENDPOINT, addr, os.environ.get("ETHERSCAN_ARMOR")))
        response_json = response.json()
        return json.loads(response_json['result'])
     
    def get_underlying_abi_from_proxy(proxy_addr):
        proxy_address = Web3.toChecksumAddress(proxy_addr)
        proxy_abi = get_abi(proxy_addr)
        proxy_w3 = w3.eth.contract(address=proxy_address, abi=proxy_abi)
        
        impl_address = proxy_w3.functions.implementation().call()
        return get_abi(impl_address)

    # extracting from PlanManager via web3
    plan_manager_proxy = "0x1337DEF1373bB63196F3D1443cE11D8d962543bB"
    plan_address = Web3.toChecksumAddress(plan_manager_proxy)
    plan_abi = get_underlying_abi_from_proxy(plan_manager_proxy)
    plan_w3 = w3.eth.contract(address=plan_address, abi=plan_abi)

    # extracting from StakeManager via web3
    stake_manager_proxy = "0x1337DEF1670C54B2a70E590B5654c2B7cE1141a2"
    stake_address = Web3.toChecksumAddress(stake_manager_proxy)
    stake_abi = get_underlying_abi_from_proxy(stake_manager_proxy)
    stake_w3 = w3.eth.contract(address=stake_address, abi=stake_abi)

    def upload_blob(bucket_name, source_file_name, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print("File uploaded to {}.".format(destination_blob_name))

    def download_blob(bucket_name, source_blob_name, destination_file_name):
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(source_blob_name)
        return blob.download_as_string()

    contract_data = json.loads(download_blob( "armor-input-files", "staking-contracts.json", None))

    contracts_all = []
    contracts_staked = []
    contracts_core_used = []

    # get timestamp at start
    ts = time.time()
    date_string = time.strftime("%Y-%m-%d", time.gmtime())
    time_string = time.strftime("%H:%M:%S", time.gmtime())

    for contract in contract_data:
        totalUsed = plan_w3.functions.totalUsedCover(
            Web3.toChecksumAddress(contract['contract_address'])
        ).call()
        totalStaked = stake_w3.functions.totalStakedAmount(
            Web3.toChecksumAddress(contract['contract_address'])
        ).call()

        contract['total_used_eth'] = totalUsed / DENOMINATOR
        contract['total_staked_eth'] = totalStaked / DENOMINATOR
     
        contract['time_stamp'] = ts
        contract['iso_date'] = date_string
        contract['time'] = time_string
     
        contracts_all.append(contract)
        if totalStaked > 0:
            contracts_staked.append(contract)
        if totalUsed > 0:
            contracts_core_used.append(contract)

    df = pd.DataFrame.from_records(contracts_all)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, '/tmp/web3-staked-sold.parquet')

    file_name = 'web3-staked-sold' + '_' + date_string + '_' + time_string
    upload_blob('armor-python-pipeline', '/tmp/web3-staked-sold.parquet', file_name)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = "armor-314014.web3_data.web3-staked-sold"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    uri = "gs://armor-python-pipeline/" + file_name
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
    
if __name__ == "__main__":
    upload_staked_sold()