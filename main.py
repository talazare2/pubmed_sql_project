import os
import gzip
import shutil
# import xml element tree 
import xml.etree.ElementTree as ET
# import sqlite3 connector 
import sqlite3 
from multiprocessing import cpu_count 
from multiprocessing.pool import ThreadPool
from converters.mesh_data_creator import mesh_db
from converters.chem_data_creator import chem_db
from converters.data_handler import db_general
from config.run_config import *
#from config.paths import db_path, xml_path


DB_PATH = '/home/tiana/Desktop/mathshub_projects/sql/sql_project/data/'

def read_xml(gz_path):
    print(f'process file {gz_path}')
    gz_id = gz_path[(gz_path.find('24n') + 3):gz_path.find('.')]
    tmp_file = DB_PATH + gz_id + '_db_tmp.xml' 
    with gzip.open(gz_path, 'rb') as f_in:
        with open(tmp_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    tree = ET.parse(tmp_file)
    # in our xml file PubmedArticle is the root for all PubmedArticle data. 
    root = tree.getroot()
    # create mesh table
    if DO_MESH:
        mesh_df = mesh_db(root)
    # create chemical table
    if DO_CHEM:
        chem_df = chem_db(root)
    # create general table
    if DO_GENERAL:
        gen_df = db_general(root)
    os.remove(tmp_file)
    return mesh_df, chem_df, gen_df

def read_parallel(args): 
    cpus = cpu_count() 
    mesh_list = []
    chem_list = []
    gen_list = []
    # to increase speed of creating independent tables do it in parallel threads, then append to list
    for results in ThreadPool(cpus - 1).imap_unordered(read_xml, args):
        mesh_list.append(results[0])
        chem_list.append(results[1])
        gen_list.append(results[2])
    return mesh_list, chem_list, gen_list

def main():
    #set all paths here to have it clear

    db = DB_PATH + 'sql_db_pm_test'
    con = sqlite3.connect(db, timeout=10)
    cur = con.cursor()

    if DO_TEST: 
        downloads_dir= '/home/tiana/Desktop/mathshub_projects/sql/sql_project/data/gz_test/'
        name_list =['pubmed24n' + (str(i).zfill(4) + '.xml.gz') for i in range(1, 20)]
        gz_path= [downloads_dir + name for name in name_list]
    else:
        downloads_dir= '/home/tiana/Desktop/mathshub_projects/sql/sql_project/data/gz_files/'
        name_list =['pubmed24n' + (str(i).zfill(4) + '.xml.gz') for i in range(1, 1220)]
        gz_path = [downloads_dir + name for name in name_list]

    #for gz in gz_path:
    #    read_xml(gz)
    mesh_list, chem_list, gen_list = read_parallel(gz_path)
    for df in mesh_list:
        df.to_sql(con=con, name='mesh_table', index=False, if_exists = 'append')
    for df in chem_list:
        df.to_sql(con=con, name='chem_table', index=False, if_exists = 'append') 
    for df in gen_list:
        df.to_sql(con=con, name='gen_table', index=False, if_exists = 'append')

if __name__ == '__main__':
    main()
