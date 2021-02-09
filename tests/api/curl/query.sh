curl -o out/query.json \
--header 'Authorization: Token 4b762569bb349bd8d60f1bc7da3f39dbfaefff9a' \
--data 'selected=objectId,gmag&tables=objects&conditions=gmag<12.0' \
https://lasair-iris.roe.ac.uk/api/query/
