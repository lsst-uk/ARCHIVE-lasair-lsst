import os
import requests, json

try:
    import settings
    token = settings.token
except:
    token    = '4b762569bb349bd8d60f1bc7da3f39dbfaefff9a'

out_file = 'tmp.json'
url      = 'https://lasair-iris.roe.ac.uk/api'

def alltest(input, method, case):
    curltest(input, method, case)
    gettest(input, method, case)
    pythontest(input, method, case)

def curltest(input, method, case):
    arglist = []
    for k,v in input.items():
        arglist.append('%s=%s' % (k,v))
    cmd = "curl -o %s " % out_file
    cmd += "--header 'Authorization: Token %s' " % token
    cmd += "--data '%s' " % '&'.join(arglist)
    cmd += "%s/%s/" % (url, method)
    print('** curl test of %s:%s' % (method, case))
    print(cmd)
    os.system(cmd)
    computed = open(out_file).read()
    try:
        json.loads(computed)
        print('---> test succeeded\n\n')
    except:
        print('---> test failed\n\n')


def gettest(input, method, case):
    arglist = []
    for k,v in input.items():
        arglist.append('%s=%s' % (k,v))
    cmd = "wget -q -O %s " % out_file
    cmd += "'%s/%s/?" % (url, method)
    cmd += "%s" % '&'.join(arglist)
    cmd += "&token=%s&format=json'" % token
    print('** get test of %s:%s' % (method, case))
    print(cmd)
    os.system(cmd)
    computed = open(out_file).read()
    try:
        json.loads(computed)
        print('---> test succeeded\n\n')
    except:
        print('---> test failed\n\n')

def pythontest(input, method, case):
    headers = { 'Authorization': 'Token %s'%token }
    url = 'https://lasair-iris.roe.ac.uk/api/'

    print('** python test of %s:%s' % (method, case))
    r = requests.post('%s/%s/'%(url, method), data=input, headers=headers)
    if r.status_code == 200:
        response = r.json()
        computed = json.dumps(response, indent=2)
        try:
            json.loads(computed)
            print('---> test succeeded\n\n')
        except:
            print('---> test failed\n\n')
    else: 
        print('---> test failed\n\n')

input = {'ra':194.494, 'dec':48.851, 'radius':240.0, 'requestType':'all'}
alltest(input, 'cone', '')

input={'objectIds':'ZTF20acpwljl,ZTF20acqqbkl,ZTF20acplggt'}
alltest(input, 'lightcurves', '')

input = {'selected':'objectId,gmag', 'tables':'objects', 'conditions':'gmag<12.0'}
alltest(input, 'query', '')

input={'objectIds':'ZTF20acpwljl,ZTF20acqqbkl,ZTF20acplggt'}
alltest(input, 'sherlock/objects', '')

input = {'ra':124.879948, 'dec':-6.020519, 'lite':True}
alltest(input, 'sherlock/position', '')

input = {'regex':'.*SN.*'}
alltest(input, 'streams', 'regex')

input = {'limit':3}
alltest(input, 'streams/2SN-likecandidates', '')
