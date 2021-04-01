import json, sys
sys.path.append('/home/ubuntu/lasair_api/lasair')
import lasair
try:
    import settings
    token = settings.token
except:
    token = '4b762569bb349bd8d60f1bc7da3f39dbfaefff9a'

ra = 194.493
dec = 48.852
objectIds = ['ZTF21aapvmcu', 'ZTF21aaopqqk']
topic = '2SN-likecandidates'

L = lasair.lasair_client(token, cache='api_cache')
#L = lasair.lasair_client(token)

print('------- cone all ------------')
c = L.cone(ra, dec, radius=240.0, requestType='all')
print(json.dumps(c, indent=2))
print('------- cone nearest ------------')
c = L.cone(ra, dec, radius=240.0, requestType='nearest')
print(json.dumps(c, indent=2))
print('------- cone count ------------')
c = L.cone(ra, dec, radius=240.0, requestType='count')
print(json.dumps(c, indent=2))

print('------- query ------------')
selected    = 'objectId, gmag'
tables      = 'objects'
conditions  = 'gmag < 12.0'
c = L.query(selected, tables, conditions, limit=10)
print(json.dumps(c, indent=2))

print('------- streams topic ------------')
c = L.streams_topics()
print(json.dumps(c, indent=2))

print('------- streams ------------')
c = L.streams(topic, limit=10)
print(json.dumps(c, indent=2))

print('-------  lightcurves ------------')
c = L.lightcurves(objectIds)
print(json.dumps(c, indent=2))

print('-------  objects ------------')
c = L.objects(objectIds)
print(json.dumps(c, indent=2))

print('-------  sherlock objects lite ------------')
c = L.sherlock_objects(objectIds, lite=True)
print(json.dumps(c, indent=2))

print('-------  sherlock objects ------------')
c = L.sherlock_objects(objectIds, lite=False)
print(json.dumps(c, indent=2))

print('-------  sherlock position lite ------------')
c = L.sherlock_position(ra, dec, lite=True)
print(json.dumps(c, indent=2))

print('-------  sherlock position ------------')
c = L.sherlock_position(ra, dec, lite=False)
print(json.dumps(c, indent=2))
