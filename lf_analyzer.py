import csv
import logging
import os.path
import uuid
import pyodbc
import sys

## *** Run as Administrator *** ##

## gets called by walk(), retrieves the paths for all the pages assoc with 
## a particular document (tocid)
def get_page_path(cnxn, tocid):
    logging.debug('Getting file paths for tocid = ' + str(tocid))

    pages = []

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
    logging.debug(sql0)

    with cnxn.cursor() as cu:
        logging.debug('Cursor instantiated')

        cu.execute(sql0)
        logging.debug('Pages SQL executed')
        pages = cu.fetchall()

    if not pages == []:
        return pages
    else:
        return 'empty'


def get_metadata(cnxn, tocid):
    logging.debug('Getting keywords for tocid = ' + str(tocid))

    rows = []

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
                    "propval pv, propdef pd ")
    sql_where = ("WHERE " +
                    "pv.tocid = " + str(tocid) + " AND " +
                    "pv.prop_id = pd.prop_id " +
                    "AND (pv.str_val IS NOT NULL " +
		              "OR pv.num_val IS NOT NULL " +
                      "OR pv.date_val IS NOT NULL);")
    
    sql = sql_select + sql_from + sql_where
    logging.debug(sql)

    with cnxn.cursor() as cu:
        logging.debug("Cursor instantiated")

        cu.execute(sql)
        logging.debug("Metadata SQL executed")
        rows = cu.fetchall()

    return rows

## TODO: status = changed, not tested
## Add an argument for "data file name" and use os.path to create it in script
def walk(cnxn, obj_id = "NULL", data_file = 'lf_data.csv'):
    
    try:
        logging.info('Begin walk() iteration for container ' + str(obj_id))

        ## Initialize the lists and variables needed to 
        ## collect the data
        docs_n_containers = []
            
        sql_select = "SELECT tocid, etype "
        sql_from = "FROM toc "
        sql_where = "WHERE parentid = " + str(obj_id) + " "
        
        if obj_id == "NULL":
            sql_where = "WHERE parentid IS NULL "
                
        sql_orderby = " ORDER BY etype asc, tocid asc;"

        sql = sql_select + sql_from + sql_where + sql_orderby
        logging.debug(sql)

        logging.debug('SQL = ' + sql)

        with cnxn.cursor() as cu:
            logging.debug('Cursor instantiated')

            cu.execute(sql)
            logging.debug('SQL executed')

            ## Once you have a resultset, transfer it into a list so the
            ## "with block" wil dispose of it when done
            docs_n_containers = cu.fetchall()
            logging.debug('Tree rows fetched')
                
            with open(data_file, 'a', newline='') as f:
                csv_w = csv.writer(f)
                csv_w.writerow('docs = -2, containers = 0')
                csv_w.writerows(docs_n_containers)
                    
            for row in docs_n_containers:
                if row[1] == -2:  #Document
                    ## DO DOCUMENT STUFF (grab pages, paths, and keywords) ##
                    ## get_page_path returns a list of lists:
                    ## [tocid, doc_name, page_num, path_name]
                    with open(data_file, 'a', newline='') as f:
                        csv_w = csv.writer(f)
                        ## TODO: status = changed, not tested
                        ## csv.writerow() takes a list.  If you give it a 
                        ## string, strings are lists so it breaks the string 
                        ## into individual characters.  Give it a list like
                        ## ['docs in container ',str(obj_id)]
                        ## 
                        csv_w.writerow(['docs in container = ', str(obj_id)])
                        csv_w.writerows(get_page_path(cnxn, row[0]))

                        ## get_page_path returns a list of lists:
                        ## [tocid, key_name, key_val]
                        csv_w = csv.writer(f)
                        ## TODO: status = changed, not tested
                        ## ['containers in container ',str(obj_id)]
                        csv_w.writerow(['containers in container ', str(obj_id)])
                        csv_w.writerows(get_metadata(cnxn, row[0]))
                    
                elif row[1] == 0: #Container
                    # Do container stuff
                    walk(cnxn, row[0], data_file)

                else:
                    #TODO: status = undecided
                    ## etype is that one "-1."  Not sure yet what to do with 
                    #  that one.  Maybe ignore since it's like 0.00001%
                    pass

    except Exception as e:
        logging.warning(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.warning(exc_type, exc_tb.tb_lineno)
## ------------- End of Functions ----------------------------


## ----------------- Script ---------------------------
try:
    log_file_name = os.path.join('s:', 'Information Technology', 'JC Misc', str(uuid.uuid1()) + '.log')
    logging.basicConfig(filename=log_file_name, 
                        filemode='w', 
                        encoding='utf-8', 
                        format='%(asctime)s.%(msecs)03d %(message)s', 
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    logging.info("Walk begins")

    ## TODO:  Status = changed, not tested
    ## Use os.path() to specify a path for the data file 
    ## ('s:\Information Technology\JC Misc\lf_data.csv')
    data_file_path = os.path.join('s:', 'Information Technology', 'JC Misc', 'lf_data.csv')
    start_container_tocid = 92498
    
    dsn_string = "DSN=LaserFicheDb"
    output_filename = "TestRun003"
    odbc_ver = "ODBC Driver 18 for SQL Server"
    trust_srvr_cert = "Yes"
    database = "Laser8"
    # mars = "Yes"
    trusted_cnxn = "Yes"
    server = "city-db2\laserf"

    cnxn_str = ("SERVER=" + server + "; " + 
                "DATABASE=" + database + "; " + 
                "DRIVER=" + odbc_ver + "; " + 
                "Trusted_Connection=" + trusted_cnxn + "; " + 
                "TrustServerCertificate=" + trust_srvr_cert + ";")

    ## Instantiate a connection that can be passed to walk()
    cnxn = pyodbc.connect(cnxn_str)

    walk(cnxn, start_container_tocid, data_file_path)

    logging.info('Walk ends')

except Exception as se:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    logging.warning(se)
    logging.warning(exc_type, exc_tb.tb_lineno)
