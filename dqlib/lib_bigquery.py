from google.cloud import bigquery

def getRowCount(bqDataset, bqTable):
    bigqueryClient = bigquery.Client()
    sql = """SELECT COUNT(1) as recordCount FROM {}.{}""".format(bqDataset, bqTable)
    job = bigqueryClient.query(sql)
    rows = job.result()
    for row in rows:
        rowCount = row.recordCount
    return(rowCount)


def getSql(bqDataset, bqTable, columnList, rowCount):
    colIdx = 0
    sql = ""
    prefix = ""
    for col in columnList:
        if colIdx != 0:
            prefix = "UNION ALL" + "\n"

        colName = """'{}'""".format(col)
        minVal = """MIN({})""".format(col)
        maxVal = """MAX({})""".format(col)
        nullVal = """COUNT(CASE WHEN {} IS NULL THEN 1 END)""".format(col)
        uniqVal = """COUNT(DISTINCT({}))""".format(col)
        selVal = """CONCAT(ROUND({}/{} * 100, 2), '%')""".format(uniqVal, rowCount)
        denVal = """CONCAT(ROUND(({} - {})/{} * 100, 2), '%')""".format(rowCount, nullVal, rowCount)

        sql = sql + prefix + """SELECT
                                    {} as ColumnName,
                                    CAST({} as String) as MinValue,
                                    CAST({} as String) as MaxValue,
                                    CAST({} as String) as NullValues,
                                    CAST({} as String) as Cardinality,
                                    CAST({} as String) as Selectivity,
                                    CAST({} as String) as Density
                                FROM {}.{}
                                """.format(colName, minVal, maxVal, nullVal, uniqVal, selVal, denVal, bqDataset, bqTable)
        colIdx = colIdx + 1
    return(sql)


def runSql(datasetName, dataTable, reportTable, sqlQuery):
    tableRef = """{}.{}""".format(datasetName, dataTable)
    reportRef = """{}.{}""".format(datasetName, reportTable)
    bigqueryClient = bigquery.Client()
    sql = """INSERT INTO {} 
                SELECT current_timestamp, '{}' as TableRef, dq.* 
                FROM ({}) dq""".format(reportRef, tableRef, sqlQuery)

    sqlJob = bigqueryClient.query(sql)
    sqlJob.result()
    return 0