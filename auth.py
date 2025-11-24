import requests
from lxml import etree
from collections import namedtuple
import truststore
truststore.inject_into_ssl()

user_agent_headers = {
'User-Agent':
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0',

}
ConfigData = namedtuple('ConfigData', [
    'login_uri',
    'login_uname_el',
    'login_passwd_el',
    'login_form_el',
    'gather_header',
    'send_header'
])

conf = ConfigData(
    login_uri = "https://isapapers.pitt.edu/cgi/users/login",
    login_uname_el = "//*[@id=\"login_username\"]",
    login_passwd_el = "//*[@id=\"login_password\"]",
    login_form_el = "//*[@id=\"login_username\"]/ancestor::r:form",
    gather_header = "Set-Cookie",
    send_header = "Cookie"
)
def login(conf, uname, passwd):
    # given xpath to input element get the form field name
    uname_xpath = f"{conf.login_uname_el}/@name" 
    passwd_xpath = f"{conf.login_passwd_el}/@name" 
    form_action_xpath = f"{conf.login_form_el}/@action"
    page = requests.get(conf.login_uri, headers=user_agent_headers)
    et = etree.fromstring(page.content)
    uname_form_name = et.xpath(uname_xpath)[0]
    passwd_form_name = et.xpath(passwd_xpath)[0]
    form_action_uri = et.xpath(form_action_xpath, 
        namespaces={'r': et.xpath("namespace-uri()")})[0]
    #print(uname_form_name, passwd_form_name, form_action_uri)
    
    post_obj = requests.post(requests.compat.urljoin(conf.login_uri,form_action_uri), data={uname_form_name: uname, 
            passwd_form_name: passwd})
    print(post_obj.content, post_obj.headers)
    
    


