import requests
from lxml import etree
from collections import namedtuple
import httpx
import truststore
from urllib.parse import urljoin
import time
truststore.inject_into_ssl()

user_agent_headers = {
'User-Agent':
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0'

}
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


def login(conf, uname, passwd):
    # given xpath to input element get the form field name
    uname_xpath = conf.login_uname_el
    passwd_xpath = conf.login_passwd_el
    form_action_xpath = conf.login_form_el
    page = httpx.get(conf.login_uri, headers=user_agent_headers)
    content = page.text
    if page.status_code == 403:
        print("Most likely Cloudflare issue.")
        '''
        stuff_start = content.index("a.src") + len("a.src = ")
        stuff_end = content.index(";", stuff_start)
        time.sleep(3)
        cf_url = content[stuff_start:stuff_end].strip("'")
        r = httpx.get(urljoin(conf.login_uri, cf_url))
        print(r.headers, r.status_code)
        '''
    #print(page.headers)
    if type(content) == bytes:
        content = content.decode("utf-8")
    #print(content)
    page.raise_for_status()
    et = etree.fromstring(content, parser=etree.HTMLParser())
    uname_form_name = et.xpath(uname_xpath)[0]
    passwd_form_name = et.xpath(passwd_xpath)[0]
    #form_action_uri = et.xpath(form_action_xpath, 
    #    namespaces={'r': et.xpath("namespace-uri()")})[0]
    form_action_uri = et.xpath(form_action_xpath) 
    #        namespaces={'r': et.xpath("namespace-uri()")})[0]
    print(uname_form_name, passwd_form_name, form_action_uri)
    
    post_ret = requests.post(requests.compat.urljoin(conf.login_uri,form_action_uri), data={uname_form_name: uname, 
            passwd_form_name: passwd})
    post_ret.raise_for_status()
    return {conf.send_header: post_ret.headers[conf.gather_header]}
    print(post_obj.content, post_obj.headers)
    
    


