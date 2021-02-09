import requests, json
headers = { 'Authorization': 'Token 4b762569bb349bd8d60f1bc7da3f39dbfaefff9a' }
url = 'https://lasair-iris.roe.ac.uk/api/sherlock/objects/'
data = {'objectIds': 'ZTF20acpwljl,ZTF20acqqbkl,ZTF20acplggt', 'lite':True}
r = requests.post(url, data, headers=headers)
print('status=', r.status_code)
if r.status_code == 200:
    response = r.json()
    s = json.dumps(response, indent=2)
    f = open('out/sherlockobjects.json', 'w')
    f.write(s)
else: 
    print(r.text)
