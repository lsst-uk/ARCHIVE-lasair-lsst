# A simple object store implemented on a file system
# Roy Williams 2020

import os
import hashlib

class objectStore():
    """objectStore.
    """

    def __init__(self, suffix='txt', fileroot='/data'):
        """__init__.

        Args:
            suffix:
            fileroot:
        """
        # make the directories if needed
        os.system('mkdir -p ' + fileroot)
        self.fileroot = fileroot
        self.suffix = suffix
    
    def getFileName(self, objectId, mkdir=False):
        """getFileName.

        Args:
            objectId:
            mkdir:
        """
        # hash the filename for the directory, use the last 3 digits
        # max number of directories 16**3 = 4096
        h = hashlib.md5(objectId.encode())
        dir = h.hexdigest()[:3]
        if mkdir:
            try:
                os.makedirs(self.fileroot+'/'+dir)
#                print('%s made %s' % (self.suffix, dir))
            except:
                pass
        return self.fileroot +'/%s/%s.%s' % (dir, objectId, self.suffix)

    def getFileObject(self, objectId):
        """getObject.

        Args:
            objectId:
        """
        f = open(self.getFileName(objectId), 'rb')
        return f

    def getObject(self, objectId):
        """getObject.

        Args:
            objectId:
        """
        try:
            f = open(self.getFileName(objectId))
            str = f.read()
            f.close()
            return str
        except:
            return None

    def putObject(self, objectId, objectBlob):
        """putObject.

        Args:
            objectId:
            objectBlob:
        """
        filename = self.getFileName(objectId, mkdir=True)
#        print(objectId, filename)
        if isinstance(objectBlob, str):
            f = open(filename, 'w')
        else:
            f = open(filename, 'wb')
        f.write(objectBlob)
        f.close()

    def getObjects(self, objectIdList):
        """getObjects.

        Args:
            objectIdList:
        """
        # get a bunch of objects from a bunch of identifiers
        D = {}
        for objectId in objectIdList:
            s = self.getObject(objectId)
            D[objectId] = json.loads(s)
        return json.dumps(D, indent=2)

    def putObjects(self, objectBlobDict):
        """putObjects.

        Args:
            objectBlobDict:
        """
        # put a bunch of objects from a dict of objectId:object
        for (objectId, objectBlob) in objectBlobDict.items():
            self.putObject(objectId, objectBlob)
