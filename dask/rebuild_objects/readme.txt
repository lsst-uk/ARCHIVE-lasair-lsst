#For full instructions see
#https://lsst-uk.atlassian.net/wiki/spaces/LUSC/pages/2888663041/Feature+Set+Evolution+for+Lasair

#Example
#dask-head   192.168.0.20
#dask-worker 192.168.0.40

mkdir csvfiles
python3 runner.py        0 2459400 csvfiles/output1.csv
python3 runner.py  2459400 2459500 csvfiles/output2.csv
python3 runner.py  2459500 2459600 csvfiles/output3.csv

#python3 csv_to_database.py         csvfiles
