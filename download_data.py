# script to download pubmed citation data from https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/
# total number of files:1219, download realized in parallel mode

import requests 
import time 
from multiprocessing import cpu_count 
from multiprocessing.pool import ThreadPool

downloads_dir= '/home/tiana/Desktop/mathshub_projects/sql/sql_project/data/gz_files/'
pm_library_url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/'
name_list =['pubmed24n' + (str(i).zfill(4) + '.xml.gz') for i in range(1, 1220)]
urls = [pm_library_url + name for name in name_list]
files = [downloads_dir + name for name in name_list]
inputs = zip(urls, files)

def download_url(args): 
  t0 = time.time() 
  url, fn = args[0], args[1] 
  try: 
    r = requests.get(url) 
    with open(fn, 'wb') as f: 
      f.write(r.content) 
      return(url, time.time() - t0) 
  except Exception as e: 
    print('Exception in download_url():', e)

def download_parallel(args): 
  cpus = cpu_count() 
  results = ThreadPool(cpus - 1).imap_unordered(download_url, args) 
  for result in results: 
    print('url:', result[0], 'time (s):', result[1])

download_parallel(inputs)