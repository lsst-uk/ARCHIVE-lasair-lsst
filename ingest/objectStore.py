# A simple object store implemented on a file system
# Roy Williams 2020

import os
import hashlib

class objectStore():
    def __init__(self, suffix='txt', fileroot='/data'):
        os.system('mkdir -p ' + fileroot)
        self.fileroot = fileroot
        self.suffix = suffix
    
    def getFileName(self, objectId, mkdir=False):
        h = hashlib.md5(objectId.encode())
        dir = h.hexdigest()[:3]
        if mkdir:
            try:
                os.makedirs(self.fileroot+'/'+dir)
#                print('%s made %s' % (self.suffix, dir))
            except:
                pass
        return self.fileroot +'/%s/%s.%s' % (dir, objectId, self.suffix)

    def getObject(self, objectId):
        f = open(self.getFileName(objectId))
        str = f.read()
        f.close()
        return str

    def putObject(self, objectId, objectBlob):
        filename = self.getFileName(objectId, mkdir=True)
#        print(objectId, filename)
        f = open(filename, 'wb')
        f.write(objectBlob)
        f.close()

    def getObjects(self, objectIdList):
        D = {}
        for objectId in L:
            s = self.getObject(objectId)
            D[objectId] = json.loads(s)
        return json.dumps(D, indent=2)

        L = []
        for objectId in objectIdList:
            L.append(self.getObject(objectId))
        return L
    
    def putObjects(self, objectBlobList):
        for (objectId, objectBlob) in objectBlobList:
            self.putObject(objectId, objectBlob)


