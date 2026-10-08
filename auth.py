# builtin imports
import requests
from collections import namedtuple
import truststore
from urllib.parse import urljoin
import time
import logging

# foreign imports
import httpx
from lxml import etree

truststore.inject_into_ssl()

user_agent_headers = {
'User-Agent':
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0'

}

user_agent_headers = {}
ConfigData = namedtuple('ConfigData', [
    'login_uri',
    'login_uname_el',
    'login_passwd_el',
    'login_form_el',
    'gather_header',
    'send_header'
])
    
def login(conf, uname, passwd, basic_auth = None):
    # given xpath to input element get the form field name
    client = None
    if client is None:
        client = httpx.Client(auth = basic_auth)
    else:
        client = httpx.Client()
    uname_xpath = conf.login_uname_el
    passwd_xpath = conf.login_passwd_el
    form_action_xpath = conf.login_form_el
    page = client.get(conf.login_uri, headers=user_agent_headers)
    cookie = page.cookies
    content = page.text
    #print(content)
    if page.status_code == 403:
        logging.warn("Permission denied to get login page. Most likely Cloudflare issue.")

    if type(content) == bytes:
        content = content.decode("utf-8")
    try:
        page.raise_for_status()
    except:
        with open("error.html", "w") as f:
            logging.err("Failed to GET login page. Writing returned html to error.html")
            f.write(content)
            raise SystemExit(1)
    et = etree.fromstring(content, parser=etree.HTMLParser())
    submit_dict = {}
    try:
        print(uname_xpath)
        uname_form_name = et.xpath(uname_xpath)[0]
        print(uname_form_name)
        passwd_form_name = et.xpath(passwd_xpath)[0]
    #form_action_uri = et.xpath(form_action_xpath, 
    #    namespaces={'r': et.xpath("namespace-uri()")})[0]
        form_element = et.xpath(form_action_xpath)[0]
        form_action_uri = form_element.get("action")
        input_elements = form_element.xpath("./input")
        
        for i in input_elements:
            print("looking at element:", i.get("id"))
            submit_dict[i.get("name")] = i.get("value")
        
        submit_dict |= {uname_form_name: uname, passwd_form_name: passwd}

    except Exception as e:
        print(e)
        print("Xpath applications failed")
        raise SystemExit(1)

    logging.info(f"form action uri used: {form_action_uri}") 
    post_ret = (
        client.post(requests.compat.urljoin(conf.login_uri,form_action_uri),
                  data = submit_dict, cookies = cookie))

    #print(post_ret, post_ret.headers, post_ret.content, sep='\n\n')
    with open("error.html", 'w') as f:
        f.write(post_ret.content.decode())
    if post_ret.status_code > 400:
        print("Failed to successfully fetch page, error noted in error.html")
        raise SystemExit(1)
    #post_ret.raise_for_status()
    return {conf.send_header: post_ret.headers[conf.gather_header]}
    
    
