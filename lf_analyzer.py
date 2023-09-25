import csv
import logging
import os.path
import uuid
import pyodbc
#import datetime

## *** Restart as Administrator *** ##

## gets called by walk(), retrieves the paths for all the pages assoc with a particular document (tocid)
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

    return pages


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
                    "pv.prop_id = pd.prop_id;")
    
    sql = sql_select + sql_from + sql_where
    logging.debug(sql)

    with cnxn.cursor() as cu:
        logging.debug("Cursor instantiated")

        cu.execute(sql)
        logging.debug("Metadata SQL executed")
        rows = cu.fetchall()

    return rows


def walk(cnxn, obj_id = "NULL"):
    
    try:
        logging.debug('Begin walk() iteration for container ' + str(obj_id))

        ## Initialize the lists and variables needed to 
        ## collect the data
        doc_list = []
        container_list = []
        results = []
            
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
            results = cu.fetchall()
            logging.debug('Tree rows fetched')
                
            for row in results:
                if row[1] == -2:  #Document
                    ## DO DOCUMENT STUFF (grab pages, paths, and keywords) ##
                    ## get_page_path returns a list of lists:
                    ## [tocid, doc_name, page_num, path_name]
                    ## TODO:  Something is happening here with the error
                    ## 'cannot access local variable 'doc_pages_n_paths' where it is not associated with a value'
                    ## Suspect get_page_path() is returning nothing.
                    ## Need to test for a "nothing" return and exclude it from doing anything
                    ## On second thought the exception block did not catch it, so it may be 
                    ## happening at line 162
                    doc_pages_n_paths = get_page_path(cnxn, row[0])

                    ## get_page_path returns a list of lists:
                    ## [tocid, key_name, key_val]
                    kwds = get_metadata(cnxn, row[0])
                    
                elif row[1] == 0: #Container
                    # Do container stuff
                    walk(cnxn, row[0])

                else:
                    #TODO: etype is that one "-1."  Not sure yet what to do with 
                    #      that one.  Maybe ignore since it's like 0.00001%
                    pass

            ## TODO: All of these inert blank lines into the file
            ## Work out why and eliminate the issue.
            ## Also, in keywords, it should only record records that actually 
            ## have keywords and not record lines where there is no kwd
            with open('results.csv', 'a') as f:
                csv_w = csv.writer(f)
                csv_w.writerows(results)

            try:
                with open('pages.csv', 'a') as f:
                    csv_w = csv.writer(f)
                    csv_w.writerows(doc_pages_n_paths)
            except Exception as ve:
                print(ve)

            with open('doc_kwds.csv', 'a') as f:
                csv_w = csv.writer(f)
                csv_w.writerows(kwds)

    except Exception as e:
        logging.debug(str(e))
        print(e)
  
## ------------- End of Functions ----------------------------


## ----------------- Script ---------------------------
logging_level = logging.info
log_file_name = os.path.join('.', str(uuid.uuid1()) + '.log')
logging.basicConfig(filename=log_file_name, 
                    filemode='w', 
                    encoding='utf-8', 
                    format='%(asctime)s.%(msecs)03d %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging_level)

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

## Instantiate a connection that can be passed to walk()
cnxn = pyodbc.connect(cnxn_str)

start_container_tocid = 1

walk(cnxn, start_container_tocid)

logging.info('Walk ends')