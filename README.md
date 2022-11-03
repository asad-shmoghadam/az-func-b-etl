# az-func-b-etl

Function B is an Azure Function to perform ETL on sensor data in EXCEL format, and store the output on Azure Blob Storage in CSV format.

## How to use

1. Create a new Azure Function App
2. Create a new Function
3. Deploy the code from this repository using VS Code or the Azure CLI
4. Create an Azure Storage Account,
5. Create a container called `attachments`
6. Create a container called `sensorsdata`
7. Add Azure Storage Account connection string for the atachments container to function configuration with the name of `ccpsattachmentsstorage_STORAGE`

> The function will be triggered if the EXCEL file added to this path `"path": "attachments/{name}.xls",`
