import csv
import datetime
import logging
import os.path
import uuid
import pyodbc

## *** Restart as Administrator *** ##

## Gets called by walk, appends the finished lists for that iteration of 
## walk() into the data file.
def list_dump(doc_info_list = [], output_filename = ""):
    
    output_filepath = output_filename + ".csv"

    with open(output_filepath, "a") as f:  
        w = csv.writer(f)                                                          
        w.writerows(doc_info_list)

## gets called by walk(), retrieves the paths for all the pages assoc with a particular document (tocid)
def get_page_path(cnxn_str, tocid):
    logging.debug('Getting file paths for tocid = ' + str(tocid))

    paths_n_pages = []

    ## doc.storeid are normal Laserfiche Documents
    ## doc.edoc_storeid are Laserfiche "E-Docs"
    sql0 = ("SELECT " +
	            "toc.tocid, " +
                "'DocumentName' = toc.name, " +
                "'PageNum' = doc.pagenum + 1, " + 
                "'FullPathAndFilename' = CASE " +
                    "WHEN edoc_storeid IS NULL " +
                    "THEN vol.fixpath + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), doc.storeid),2),1,2) + '\' + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), doc.storeid),2),3,2) + '\' + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), doc.storeid),2),5,2) + '\' + " +
                        "CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), doc.storeid),2) + '.TIF' " +
                    "ELSE vol.fixpath + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), edoc_storeid),2),1,2) + '\' + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), edoc_storeid),2),3,2) + '\' + " +
                        "SUBSTRING(CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), edoc_storeid),2),5,2) + '\' + " +
                        "CONVERT(VARCHAR(8),CONVERT(VARBINARY(4), edoc_storeid),2) + edoc_ext " +
                    "END " +
            "FROM " +
                "dbo.toc LEFT JOIN " +
                "dbo.doc ON dbo.doc.tocid = dbo.toc.tocid LEFT JOIN " +
                "dbo.vol ON dbo.toc.vol_id = dbo.vol.vol_id " +
            "WHERE " +
                "dbo.toc.tocid = " + str(tocid) + " AND " +
                "doc.pagenum >= 0 " +
            "ORDER BY " +
                "doc.pagenum ASC;")
    
    cxn_pnp = pyodbc.connect(cnxn_str)
    logging.debug('Connection successful')
    cxn_pnp.execute('USE Laser8;')

    cu = cxn_pnp.cursor()
    logging.debug('Cursor instantiated')

    cu.execute(sql0)
    logging.debug('SQL executed')
    row = cu.fetchone()

    while row:
        paths_n_pages.append([row[0], row[1], row[2], row[3]])

    return paths_n_pages


def get_metadata(cnxn_str, tocid):
    logging.debug('Getting keywords for tocid = ' + str(tocid))

    kwds = []

    sql_select = ("SELECT " +
                    "pv.tocid, "
                    "'Keyword Name' = pd.prop_name, " +
                    "'Keyword Value' = CASE " +
                        "WHEN pd.prop_type IN ('L','S') " + 
                            "THEN pv.str_val " +
                        "WHEN pd.prop_type IN ('I','N') " + 
                            "THEN CONVERT(VARCHAR(20), pv.num_val) " +
                        "WHEN pd.prop_type = 'D' " +
                            "THEN CONVERT(VARCHAR(20), pv.date_val, 120) " +
                    "END ")
    sql_from = ("FROM " +
                    "propval pv, propdef pd")
    sql_where = ("WHERE " +
                    "pv.tocid = " + str(tocid) + " AND " +
                    "pv.prop_id = pd.prop_id;")
    
    sql = sql_select + sql_from + sql_where

    cxn_kwrd = pyodbc.connect(cnxn_str)
    logging.debug("Connection successful")
    cxn_kwrd.execute("USE Laser8;")

    cu = cxn_kwrd.cursor()
    logging.debug("Cursor instantiated")

    cu.execute(sql)
    logging.debug("SQL executed")
    row = cu.fetchone()

    while row:
        logging.debug("Metadata row fetched")
        kwds.append([row[0], row[1], row[2]])

    return kwds


def walk(cnxn_str, obj_id = "NULL", data_filename = ""):
    logging.debug('Begin walk() iteration for container ' + str(obj_id))

    ## Instantiates a connection object unique to this walk() call on the stack
    cxn = pyodbc.connect(cnxn_str)
    logging.debug('Connection successful')
    cxn.execute('USE Laser8;')
    logging.debug('Laser8 set as default db')

    ## Initialize the lists and variables needed to 
    ## collect the data
    doc_list = []
    container_list = []
         
    sql_select = "SELECT tocid, etype "
    sql_from = "FROM toc "
    sql_where = "WHERE parentid = " + str(obj_id) + " "
    
    if obj_id.strip() == "NULL":
        sql_where = "WHERE parentid IS NULL "
               
    sql_orderby = " ORDER BY tocid asc;"

    sql = sql_select + sql_from + sql_where + sql_orderby

    logging.debug('SQL = ' + sql)

    cu = cxn.cursor()
    logging.debug('Cursor instantiated')

    cu.execute(sql)
    logging.debug('SQL executed')
    row = cu.fetchone()

    while row:
        logging.debug('Tree row fetched')
        page_rows = []
        if row[1] == -2:  #Document
            ## DO DOCUMENT STUFF ##
            ## TODO: Send this tocid to get_page_path() and get the list 
            ##       of pages for this doc
            ## get_page_path returns a list of lists:
            ## [tocid, doc_name, page_num, path_name]
            doc_pages_n_paths = get_page_path(cnxn_str, row[0])

            ## TODO: Send this tocid to get_metadata() and get the metadata
            ##       for this doc
            ## get_page_path returns a list of lists:
            ## [tocid, key_name, key_val]
            doc_kwds = get_metadata(cnxn_str, row[0])
            
            for rec in doc_kwds:
                doc_list.append(row[0])

        elif row[1] == 0: #Container
            # Do container stuff
            container_list.append([row[0]])
            container_count += 1
            walk(cnxn_str, row[0], data_filename)

        else:
            #TODO: etype is that one "-1."  Not sure yet what to do with 
            #      that one.  Maybe ignore since it's like 0.00001%
            pass

        row = cu.fetchone()

    list_dump(doc_list, container_list, data_filename)    


log_file_name = os.path.join('.', str(uuid.uuid1()) + '.log')
logging.basicConfig(filename=log_file_name, 
                    filemode='w', 
                    encoding='utf-8',
                    level=logging.DEBUG, 
                    format='%(asctime)s %(message)s', 
                    datefmt='%m/%d/%Y %I:%M:%S %p')

logging.info("Walk begins")

dsn_string = "DSN=LaserFicheDb"

output_filename = "TestRun003"

odbc_ver = "ODBC Driver 18 for SQL Server"
trust_srvr_cert = "Yes"
database = "Laser8"
mars = "Yes"
trusted_cnxn = "Yes"
server = "city-db2\laserf"

cnxn_str = ("SERVER=" + server + "; " + 
            "DATABASE=" + database + "; " + 
            "DRIVER=" + odbc_ver + "; " + 
            "Trusted_Connection=" + trusted_cnxn + "; " + 
            "TrustServerCertificate=" + trust_srvr_cert + "; " + 
            "MultipleActiveResultSets=" + mars)

start_container_tocid = 1

try:
    walk(cnxn_str, start_container_tocid, output_filename)
except Exception as e: 
    logging.debug(str(e))

logging.info('Walk ends')