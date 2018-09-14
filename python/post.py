import httplib2
import json
import base64
from simplejson import loads, dumps
#urlbase="http://localhost:8082/openmrs/ws/rest/v1/obs"
# parse an xml file by name
mydoc = open('obs3.json', "r")
doc = mydoc.read()
parsed_json = json.loads(doc)
print(dumps(parsed_json));
user = 'apiuser'
passwd = 'darWodcikunhij9'
uri = "http://localhost:8082/openmrs/ws/fhir/Observation/"
h = httplib2.Http()
h.add_credentials(user, passwd)
h.follow_all_redirects = True
auth = base64.encodestring( user + ':' + passwd )
headers = { 'Authorization' : 'Basic ' + auth ,'Content-Type': 'application/json; charset=UTF-8'}
#print(json.dumps(parsed_json))
resp, content = h.request(uri, "POST", body=dumps(parsed_json), headers=headers)
print(resp);
