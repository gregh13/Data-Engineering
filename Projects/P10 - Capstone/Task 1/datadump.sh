#!bin/bash
echo 'Accessing MySQL Database to export sales data'

mysqldump -uroot -pMjk3MTEtZ3JlZ2hl --no-create-info sales > sales_data.sql


echo 'Finished'
