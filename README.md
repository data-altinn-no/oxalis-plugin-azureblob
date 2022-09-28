> Note! This repository is not maintained and may contain vulnerabilities. Handle with care.

# oxalis-plugin-azureblob

## About

This is a Oxalis 4 persistor plugin that pushes incoming messages and receipts to a Azure Storage Account. 

## Installation

1. Compile and place .jar in the directory defined by `oxalis.path.plugin` in `oxalis.conf`
2. Set `oxalis.persister.receipt = plugin` and `oxalis.persister.payload = plugin` in `oxalis.conf`
3. Set the environment variable `AZURE_STORAGE_ACCOUNT_CONNECTION_STRING` to the connection string to the Azure Storage Account

Oxalis should now start pushing incoming messages and receipts to Azure
