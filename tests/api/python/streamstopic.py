import requests,json
headers = { 'Authorization': 'Token a8f6df0e27618012c7cfbd957e8db267b82dc8c6' }
url = 'https://lasair-iris.roe.ac.uk/api/streams/2SN-likecandidates/'
data = { 'limit': 3 }
r = requests.post(url, data, headers=headers)
print('status=', r.status_code)

if r.status_code == 200:
    response = r.json()
    s = json.dumps(response, indent=2)
    f = open('out/streamstopic.json', 'w')
    f.write(s)
else: 
    print(r.text)


