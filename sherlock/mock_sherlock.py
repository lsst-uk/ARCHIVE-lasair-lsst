# mock version of sherlock API for testing

class transient_classifier:
    def __init__(
            self,
            log,
            settings,
            ra,
            dec,
            name,
            verbose=1,
            updateNed=False
            ):
        self.log=log
        self.settings=settings
        self.ra=ra
        self.dec=dec
        self.name=name
        if len(ra) != len(dec):
            raise ValueError("Length of ra and dec must be equal")
        if name != None and len(name) != len(ra):
            raise ValueError("Length of name, ra and dec must be equal")

    def classify(self):
        classifications = {}
        crossmatches = []
        for (name, ra, dec) in zip(self.name, self.ra, self.dec):
            #print ("{:s} {:f} {:f}".format(name, ra, dec))
            sum = ra + dec
            classifications[name] = 'XX'
            crossmatches.append({ 'transient_object_id':name, 'sum':sum })
        return classifications, crossmatches
