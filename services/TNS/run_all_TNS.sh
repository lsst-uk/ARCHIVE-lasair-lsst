for i in {41..50}
do
    echo '*** page ' $i
    python poll_tns.py --pageNumber=$i --pageSize=500
done

