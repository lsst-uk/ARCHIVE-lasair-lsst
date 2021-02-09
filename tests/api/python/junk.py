import requests, json
headers = { 'Authorization': 'Token 4b762569bb349bd8d60f1bc7da3f39dbfaefff9a' }
headers = { 'Authorization': 'Token a8f6df0e27618012c7cfbd957e8db267b82dc8c6' }
url = 'https://lasair-iris.roe.ac.uk/api/cone/'
data = {'ra': '194.494', 'dec': '48.851', 'radius': '240', 'requestType':'all' }

r = requests.post(url, data, headers=headers)
if r.status_code == 200:
    response = r.json()
    s = json.dumps(response, indent=2)
    print(s)
else: 
    print(r.status_code)
    print(r.text)

