echo 'curl tests';         cd curl
echo '  cone';             sh cone.sh
echo '  lightcurves';      sh lightcurves.sh
echo '  query';            sh query.sh
echo '  sherlockobjects';  sh sherlockobjects.sh
echo '  sherlockposition'; sh sherlockposition.sh
echo '  streamsregex';     sh streamsregex.sh
echo '  streamstopic';     sh streamstopic.sh

echo 'get tests';          cd ../get
echo '  cone';             sh cone.sh
echo '  lightcurves';      sh lightcurves.sh
echo '  query';            sh query.sh
echo '  sherlockobjects';  sh sherlockobjects.sh
echo '  sherlockposition'; sh sherlockposition.sh
echo '  streamsregex';     sh streamsregex.sh
echo '  streamstopic';     sh streamstopic.sh

echo 'python tests';       cd ../python
echo '  cone';             python cone.py
echo '  lightcurves';      python lightcurves.py
echo '  query';            python query.py
echo '  sherlockobjects';  python sherlockobjects.py
echo '  sherlockposition'; python sherlockposition.py
echo '  streamsregex';     python streamsregex.py
echo '  streamstopic';     python streamstopic.py
