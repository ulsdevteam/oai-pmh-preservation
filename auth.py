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

_conf = ConfigData(
    login_uri = "https://isapapers.pitt.edu/cgi/users/login",
    login_uname_el = "//*[@id=\"login_username\"]",
    login_passwd_el = "//*[@id=\"login_password\"]",
    login_form_el = "//*[@id=\"login_username\"]/ancestor::r:form",
    gather_header = "Set-Cookie",
    send_header = "Cookie"
)

_conf = ConfigData(
    login_uri = "https://hyku7-pittir-test-tenant.palni-palci-knapsack-friends.notch8.cloud/users/sign_in?locale=en",
    login_uname_el = "//*[@id=\"login_username\"]",
    login_passwd_el = "//*[@id=\"login_password\"]",
    login_form_el = "//*[@id=\"login_username\"]/ancestor::r:form",
    gather_header = "Set-Cookie",
    send_header = "Cookie"
)
    
    


def login(conf, uname, passwd):
    # given xpath to input element get the form field name
    uname_xpath = conf.login_uname_el
    passwd_xpath = conf.login_passwd_el
    form_action_xpath = conf.login_form_el
    page = httpx.get(conf.login_uri, headers=user_agent_headers)
    cookie = page.cookies
    content = page.text
    #print(content)
    if page.status_code == 403:
        logging.warn("Permission denied to get login page. Most likely Cloudflare issue.")

    if type(content) == bytes:
        content = content.decode("utf-8")
    page.raise_for_status()
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
        requests.post(requests.compat.urljoin(conf.login_uri,form_action_uri),
                  data = submit_dict, cookies = cookie))

    #print(post_ret, post_ret.headers, post_ret.content, sep='\n\n')
    with open("error.html", 'w') as f:
        f.write(post_ret.content.decode())
    post_ret.raise_for_status()
    return {conf.send_header: post_ret.headers[conf.gather_header]}
    
    

if __name__ == '__main__':
    print(login(_conf, "pals", "pals"))
