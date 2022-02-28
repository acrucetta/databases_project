import json
import argparse
import sys
import requests
from requests.exceptions import ConnectionError, ConnectTimeout

load_url="inspections"


#TODO extract commont function

def run_loader(config):
    with open(config.file) as jfile:
        json_input = json.load(jfile)
        post_url = "http://%s:%s/%s" % (config.server,config.port,load_url)
        print("Using post url to load %s" % post_url)
        if config.single:
            try:
                r = requests.post(post_url,json=json_input)
                
                if r.status_code > 400:
                    print("Error.  %s  Body: %s" % (r,r.content))
                else: 
                    print("Resp: %s  Body: %s" % (r,r.content))
            except ConnectionError as err:
                print("Connection error, halting %s" % err)
                return
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise            
        else:
            for x in json_input:
                try:
                    r = requests.post(post_url,json=x,)                    
                    if r.status_code > 400:
                        print("Error.  %s  Body: %s" % (r,r.content))
                    else: 
                        print("Resp: %s  Body: %s" % (r,r.content))
                except ConnectionError as err:
                    print("Connection error, halting %s" % err)
                    return
                except:
                    print("Unexpected error:", sys.exc_info()[0])
                    raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--file", dest="file", help="Input json file",required=True)
    parser.add_argument("-s","--server", help="Server hostname (default localhost)",default="localhost")
    parser.add_argument("-p","--port", help="Server port (default 30235)",default=30235, type=int)
    parser.add_argument("--single", help="Call a loader for a JSON file with a single entry",action="store_true")
    config = parser.parse_args()
    run_loader(config)


