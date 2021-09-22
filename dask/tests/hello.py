import sys
import time
import dask.bag as db

mjd_now = time.time()/86400 + 40587.0
jd_now = mjd_now + 2400000.5
#print('jd_now is ', jd_now)

def crunch(w):
    return w*10

def main():
    ws = [2,3,4,5,6,7, 34, 35, 36, 37, 38, 23, 22, 24, 25, 26, 99]
    wdictlist = []
    for w in ws:
        wdictlist.append(w)

    bag = db.from_sequence(wdictlist, npartitions=1)
    fbag = bag.map(crunch)

    x = fbag.take(8)
    print(x)

if __name__ == "__main__":
    main()
