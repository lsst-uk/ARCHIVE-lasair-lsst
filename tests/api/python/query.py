import requests
import json
headers = { 'Authorization': 'Token 4b762569bb349bd8d60f1bc7da3f39dbfaefff9a' }
url = 'https://lasair-iris.roe.ac.uk/api/query/'
data = {
'selected'  : 'objectId, gmag',
'tables'    : 'objects',
'conditions': 'gmag < 12.0',
'limit'     : 10
}
r = requests.post(url, data, headers=headers)
if r.status_code == 200:
    response = r.json()
    s = json.dumps(response, indent=2)
    f = open('out/query.json', 'w')
    f.write(s)
else: 
    print(r.text)
