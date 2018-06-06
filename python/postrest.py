import httplib2
import json
from simplejson import loads, dumps
#urlbase="http://localhost:8082/openmrs/ws/fhir/Encounter"
urlbase="http://localhost:8082/openmrs/ws/rest/v1/obs"
# parse an xml file by name
mydoc = open('obs.json', "r")
doc = mydoc.read()
parsed_json = json.loads(doc)
print(dumps(parsed_json));
user = 'apiuser'
passwd = 'darWodcikunhij9'
uri = "http://localhost:8082/openmrs/ws/fhir/Encounter/"
h = httplib2.Http()
h.add_credentials(user, passwd)
h.follow_all_redirects = True
headers={'Content-Type': 'application/json; charset=UTF-8'}
#print(json.dumps(parsed_json))
resp, content = h.request(uri, "POST", body=dumps(parsed_json), headers=headers)
print(resp);
