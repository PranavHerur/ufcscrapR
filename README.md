# Please clone/download the ufcscrapR-data repo first
https://www.github.com/pranavherur/ufcscrapR-data 

Then clone this repo at the same level

    project_folder
        |---- ufcscrapR-data
        |---- ufcscrapR


example usage\
will check for crawled fights and minimize to new only (full fight crawl takes 5 hours)
    
    from ufcscrapR.crawl import crawl_fighters, crawl_event_list, crawl_events
    from ufcscrapR.crawl_fights import crawl_fights
    
    
    # crawl rarely, data doesn't change often
    # files have the fighter names anyway
    crawl_fighters()
    
   
    # updates trigger file. need events to get fights. need fights to get fight stats
    crawl_event_list()
     
     # this function gets the stats
    crawl_events()
    
