import requests, json
headers = { 'Authorization': 'Token 4b762569bb349bd8d60f1bc7da3f39dbfaefff9a' }
url = 'https://lasair-iris.roe.ac.uk/api/sherlock/position/'
data = {'ra':115.811388, 'dec':-25.764042, 'lite':True}

r = requests.post(url, data, headers=headers)
if r.status_code == 200:
    response = r.json()
    s = json.dumps(response, indent=2)
    f = open('out/sherlockposition.json', 'w')
    f.write(s)
else: 
    print(r.text)
