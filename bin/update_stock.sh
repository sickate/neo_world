#!/bin/bash
start_date=$1
end_date=`date "+%Y%m%d"`
echo $start_date
echo $end_date
echo "Start updating at `date`..."

python ayn.py data check  -s $start_date -e $end_date

#python ayn.py data daily_basic -s $start_date -e $end_date
#python ayn.py data adj_factor -s $start_date -e $end_date
#python ayn.py data price -s $start_date -e $end_date
#python ayn.py data money -s $start_date -e $end_date
#python ayn.py data money_jq -s $start_date -e $end_date
#python ayn.py data dragon -s $start_date -e $end_date
#python ayn.py data dragon_jq -s $start_date -e $end_date
#python ayn.py data shibor -s $start_date -e $end_date
#python ayn.py data auction -s $start_date -e $end_date
#python ayn.py data stock_share -s $start_date -e $end_date
echo "Done updating at `date`..."
