import yaml
from dqlib import lib_bigquery

configFile = "./config/dq.yaml"
configList = yaml.safe_load(open(configFile))

for tableConf in configList:
    datasetName = tableConf['datasetName']
    dataTable = tableConf['dataTable']
    reportTable = tableConf['reportTable']
    columnList = tableConf['includeColumns']

    # Get Row Count
    rowCount = lib_bigquery.getRowCount(datasetName, dataTable)

    # Prepare Dynamic SQL Statement
    sqlQuery = lib_bigquery.getSql(datasetName, dataTable, columnList, rowCount)

    # Run SQL & Display Output
    lib_bigquery.runSql(datasetName, dataTable, reportTable, sqlQuery)
