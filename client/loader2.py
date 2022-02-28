import json
import argparse
import sys
import requests
from requests.exceptions import ConnectionError, ConnectTimeout
from collections import defaultdict
from os import path


class LoaderError(Exception):
    def __init__(self, message=None):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Loader Error"


# Validate the script file
def validate_script(script_file):
    with open(script_file, 'r') as file_in:
        script_dir = path.dirname(script_file)
        json_script = json.load(file_in)
        if not isinstance(json_script, list):
            raise LoaderError("Main/root JSON object in the file %s is not a list" % script_file)
        for script_obj in json_script:
            if not isinstance(script_obj, object):
                raise LoaderError("Expected object in list file")
            if "url" in script_obj and "response" in script_obj:
                pass
            elif "file" in script_obj:
                if not path.exists(path.join(script_dir, script_obj["file"])):
                    raise LoaderError("File given but does not exist %s" % script_obj["file"])
                with open(path.join(script_dir, script_obj["file"]), 'r') as test_file:
                    test_file_json = json.load(test_file)
                    if "response" not in test_file_json:
                        raise LoaderError("Test script file %s missing response %s"
                                          % (test_file, test_file_json.keys()))
                    if "post_path" and "values" in test_file_json:
                        pass
                    elif "get_path" and "tests" in test_file_json:
                        pass
                    else:
                        raise LoaderError("Test script file %s should have post_path & values, or get_path & tests %s"
                                          % (test_file, test_file_json.keys()))


# Run a single file which is made up of multiple requests to the same URL
def run_test_file(server, test_file_path, fail_on_wrong_response=True):
    with open(test_file_path, 'r') as test_file:
        script = json.load(test_file)
        response = script["response"]
        if not isinstance(response, list):
            response = [ response ]
        if "post_path" in script:
            count = 0
            post_url = "%s%s" % (server, script["post_path"])
            for v in script["values"]:
                r = requests.post(post_url, json=v)
                if r.status_code not in response:
                    msg = "Failure (got %s, expected %s) on post to %s with value: %s. Body: %s " % (r.status_code, response, post_url, v, r.content)
                    if fail_on_wrong_response:
                        raise LoaderError(msg)
                    else:
                        print(msg)
                else:
                    count += 1
        elif "get_path" in script:
            count = 0
            get_urlbase = "%s%s" % (server, script["get_path"])
            for v in script["tests"]:
                if "inputs" in v:
                    inputs = v["inputs"]
                    get_url = "%s/%s" % (get_urlbase, str(inputs))
                else:
                    get_url = get_urlbase
                # appending parameters into get_url
                expected = v["expected"]

                r = requests.get(get_url)
                if r.status_code not in response:
                    msg = "Failure (%s) on get to %s. Expected %s  " % (r.status_code, get_url, response)
                    if fail_on_wrong_response:
                        raise LoaderError(msg)
                    else:
                        print(msg)
                else:
                    res = r.json()
                    if isinstance(expected, list) and not isinstance(res, list):
                        res = [res]
                    if expected != res:
                        if fail_on_wrong_response:
                            if config.indent:
                                expected_out = json.dumps(expected, indent=1)
                                res_out = json.dumps(res, indent=1)
                            else:
                                expected_out = expected
                                res_out = res
                            msg = "Wrong expected value on get to %s. \nExpect:%s\nGot   :%s  " % (get_url, expected_out, res_out)
                            if fail_on_wrong_response:
                                raise LoaderError(msg)
                            else:
                                print(msg)

                        print("===unexpected return at" + get_url + "===")
                        print("expected json: %s" % expected)
                        print("actual: %s" % r.json())
                        print("=======================")
                    else:
                        count += 1
    return count


# Run the script file that contains a list of URLS and file for testing
def run_script(script_file, cfg):
    print("Running script %s" % script_file)
    server = "http://%s:%s/" % (cfg.server, cfg.port)
    script_dir = path.dirname(script_file)
    with open(script_file, 'r') as file_in:
        json_script = json.load(file_in)
        for script in json_script:
            if "url" in script:
                get_url = "%s%s" % (server, script["url"])
                r = requests.get(get_url)
                if r.status_code != script["response"]:
                    msg = "Failure on %s. Expected status code %s Got %s" % (get_url, script["response"], r.status_code)
                    if cfg.halt:
                        raise LoaderError(msg)
                    else:
                        print(msg)
                else:
                    print("Called %s" % get_url)
                    if "body" in script:
                        resp = r.content.decode("utf-8")
                        if script["body"] != resp:
                            msg = "Was looking for %s on %s. Instead got %s" % (script["body"], get_url, resp)
                            if cfg.halt:
                                raise LoaderError(msg)
                            else:
                                print(msg)
                        else:
                            print("Expected content matched")
                    elif "jsonbody" in script:
                        resp = r.json()
                        if script["jsonbody"] != resp:
                            if cfg.indent:
                                expected_out = json.dumps(script["jsonbody"], indent=1)
                                res_out = json.dumps(resp, indent=1)
                            else:
                                expected_out = script["jsonbody"]
                                res_out = resp
                            msg = "Was looking for\n%s\non %s. Instead got:\n%s" % (expected_out, get_url, res_out)
                            if cfg.halt:
                                raise LoaderError(msg)
                            else:
                                print(msg)
                        else:
                            print("Expected content matched")
            else:
                count = run_test_file(server, path.join(script_dir, script["file"]), cfg.halt)
                print("Ran file %s Successful %s" % (script["file"], count))
    print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", dest="file", help="Input json script file", required=True)
    parser.add_argument("-s", "--server", help="Server hostname (default localhost)", default="localhost")
    parser.add_argument("-p", "--port", help="Server port (default 5000)", default=30235, type=int)
    parser.add_argument("-i", "--indent", help="indent compare output (default False)", default=False,
                        action="store_true")
    parser.add_argument("--halt", help="halt on error (default False)", default=False,
                        action="store_true")
    config = parser.parse_args()
    try:
        validate_script(config.file)
        run_script(config.file, config)
    except LoaderError as e:
        print("LoaderError: %s" % e.message)
