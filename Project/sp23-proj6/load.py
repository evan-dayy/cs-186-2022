import shutil
import subprocess
import os

port = 27017

if not os.path.exists('data'):
    print("Can't find data/ directory. Did you unzip data.zip?")
    exit(1)

for dname in ['credits', 'movies_metadata', 'keywords', 'ratings']:
    fpath = os.path.join('data', dname+'.dat')
    if not os.path.exists(fpath):
        print("Can't find `data/{}.dat`. Make sure that all the mjson files are in root of your data/ directory".format(dname))
        exit(1)
    subprocess.run(["mongoimport", "--type", "json", "-d", "movies", "-c", dname, "--drop", "--port", str(port), fpath])
