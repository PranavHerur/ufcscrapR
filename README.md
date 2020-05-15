# Please download from the ufcscrapR-data repo first
https://www.github.com/pranavherur/ufcscrapR-data 

Then clone this repo at the same level

ex:\
project_folder\
        |---- ufcscrapR-data\
        |---- ufcscrapR


example usage\
will check for crawled fights and minimize to new only (full fight crawl takes 5 hours)\
also checks that fights have happened before today

start = timer()\
from ufcscrapR.crawl import crawl_fighters, crawl_event_list, crawl_events\
from ufcscrapR.crawl_fights import crawl_fights

crawl_fighters()\
crawl_event_list()\
crawl_events()\

print(round(timer() - start, 2), "seconds")