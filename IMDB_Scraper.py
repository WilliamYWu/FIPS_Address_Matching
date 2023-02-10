'''
Name: William Wu
Date: 10/30/2022
Description: Grabs specific IMDB Metadata, some slight preprocessing and saves into file for enricher

Requirements: Install packages and change MAIN_DIR in "region: GLOBAL constants" to run
'''

# region: Setup Packages
import os
from datetime import datetime
import time
import psutil
import logging
import logging.handlers
from functools import wraps
# endregion: Setup Packages

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

# region: GLOBAL constants
START_YEAR = 1995
END_YEAR = 2021

# High Level Directory (ex. "C:\\Code\\Project")
MAIN_DIR = "D:\Code\Project"

LOG_DIR = MAIN_DIR + f"\\Log\\{datetime.now().strftime('%Y%m%d')}"
LOG_FILE = LOG_DIR + f"\\Log_{datetime.now().strftime('%H%M%S')}.log"

DIR_LIST: list[str] = [LOG_DIR]
YEARS: list[str] = []

# endregion: GLOBAL constants

def timeit_memoryusage(method):
    '''
    DESCRIPTION -> Wrapper function to time and detect memory usage

    PARAM 1 -> method -> function
    '''
    @wraps(method)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        end_memory = process.memory_info().rss
        logging.info(f"{method.__name__} Time Taken => {(end_time-start_time)*1000} ms")
        logging.info(f"{method.__name__} Memory Used => {(end_memory-start_memory)} bytes")
        return result
    return wrapper

def directory_setup(dir_list):
    '''
    DESCRIPTION > If the directory does not exist it will create it
    '''
    for directory in dir_list:
        if not os.path.exists(directory):
            os.makedirs(directory)


def logging_setup():
    '''
    DESCRIPTION -> Setups the logging file for code
    '''
    try:
      handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", LOG_FILE))
      formatter = logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
      handler.setFormatter(formatter)
      logging.getLogger().handlers.clear()
      root = logging.getLogger()
      root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
      root.addHandler(handler)
      logging.propogate = False
      logging.info("Log File was created successfully.")
    except Exception as e:
        exit

def date_range(start, end):
    '''
    DESCRIPTION -> Creates a list of years from the given date range

    PARAM 1 -> start -> Start year
    PARAM 2 -> end -> End year
    '''
    logging.info(f"Start Year: {start}")
    logging.info(f"End Year: {end}")
    for year in range(start,end+1):
        YEARS.append(year)

@timeit_memoryusage
def scrape_metadata(soup, tag, key, regex_value, wrong_tag, wrong_key, wrong_regex):
    '''
    DESCRIPTION -> Scrapes metadata from website (In this case Imdb Movie ranking) and organizes it into a dataframe

    PARAM 1 -> soup -> HTML text, preferrable processed by BeautifulSoup
    PARAM 2 -> tag -> Wanted tag
    PARAM 3 -> key -> Wanted key within tag
    PARAM 4 -> regex_value -> Wanted text value within key, needs to be written as a Regex Expression
    PARAM 5 -> wrong_tag -> If unwanted duplicates exist specifiy the tag here
    PARAM 6 -> wrong_key -> Unwanted key within tag
    PARAM 7 -> wrong_regex -> Unwanted text value within key
    '''
    # gets the right content
    try:
        contents = soup.find_all(lambda element: element.name == tag and re.findall(regex_value, str(element.get(key))))
        logging.info(f"Successfully found content.")
    except Exception as e:
        logging.error(f"Failed to find content: {e}")
    if wrong_tag:
        try:
            [contents.remove(content) for content in contents if content.find(wrong_tag, {wrong_key: wrong_regex})] 
            logging.info(f"Successfully removed content.")
        except Exception as e:
            logging.error(f"Failed to remove content: {e}")
    # Stores in dictionary and creates dataframe
    dict = {}
    for i, content in enumerate(contents):
        key = (i) % 50
        dict.setdefault(key, []).append(' '.join(content.text.strip().split()))
    df = pd.DataFrame(list(dict.values()), columns = ['value']) 
    return df

@timeit_memoryusage
def main():
    directory_setup(DIR_LIST)
    logging_setup()
    date_range(START_YEAR,END_YEAR)

    raw_df = pd.DataFrame()
    for year in YEARS:
        main_url = f"http://imdb.com/search/title/?year={year}&title_type=feature&sort=boxoffice_gross_us,desc"
        # Gets the top 50 movies by default
        scrape_link = requests.get(main_url)
        logging.info(f"---------------------IMDB Movie Ranking {year}-------------------")
        soup = BeautifulSoup(scrape_link.text, "html.parser", multi_valued_attributes=None)

        # Get metadata information like rank, name, and etc.
        logging.info("Get movie metadata: rank")
        rank = scrape_metadata(soup, "span", "class", re.compile(r"^lister-item-index\sunbold\stext-primary$"), False, False, False).rename(columns={"value":"rank"})
        logging.info("Get movie metadata: name")
        name = scrape_metadata(soup, "a", "href", re.compile(r"^\/title\/[a-z]{2}[0-9]+\/$"), "img", "height", re.compile(r"^[0-9]{2}$")).rename(columns={"value":"name"})
        
        # Create lower dimension df to concat into larger df
        merge_df = pd.DataFrame()
        merge_df = pd.concat([rank, name], axis=1)
        merge_df['year'] = year
        
        raw_df = pd.concat([raw_df, merge_df])
    
    # Process Data
    logging.info("Clean and Process data.")
    raw_df['rank'] = raw_df['rank'].apply(lambda x: str(x).replace('.',''))
    raw_df.reset_index(drop=True, inplace=True)
    raw_df.index.name = 'movie_id'
    logging.info("-----------------Final Dataframe------------------")
    logging.info(raw_df)
    # Done
    output_dir = "..\\hollywood-diversity\\data\\box_office_top_50_movies_1995-2021.csv"
    raw_df.to_csv(output_dir)
    return

if __name__ == "__main__":
    main()
