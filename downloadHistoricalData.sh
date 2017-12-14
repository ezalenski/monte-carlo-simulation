#!/usr/bin/env bash
#
# download daily closing prices for select stocks from 2000 to present
#  $0 HDFS_location
#  ex ./downloadHistoricalData.sh hdfs://sandbox.hortonworks.com:8020/tmp/stockData/
#
destinationDir=${1:-/tmp/stockData/}

echo Running as $(id -un) on $(uname -n):$(pwd -P)
echo Will save data to $destinationDir

fromMonth=01
toMonth=11
fromYear=1990
toYear=2017
fromDay=01
toDay=30
t=$(mktemp -d /tmp/downloadStock-input-XXXXX)
i=0
oldestCompanies=$(cat companies_list.txt |  egrep -v '^(#.*|\s+)$' | tail -n +2 | cut -d',' -f1)
for s in $oldestCompanies; do

 #columns are: Date,Open,High,Low,Close,Volume,Adj Close
 url="https://www.quandl.com/api/v3/datasets/WIKI/$s.csv?start_date=$fromYear-$fromMonth-$fromDay&end_date=$toYear-$toMonth-$toDay"
 echo Downloading historical data for $s

 (
 curl -s "$url" | cut -d, -f7-11,13 --complement | tail -n +2  | tac > $t/${s}.csv

 #add Symbol and Change % columns to csv
 lastPrice=
 echo "Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct" > $t/${s}_2.csv
 for l in $(cat $t/${s}.csv  ); do
    currentPrice=${l##*,}
    changeInPrice=0
    if [ -n "$lastPrice" ]; then
#        changeInPrice=$(perl -e "printf('%.4f',($currentPrice/$lastPrice - 1)*100)")
        changeInPrice=$(bc -l <<< "scale=4;($currentPrice/$lastPrice -1)*100")
    fi
    lastPrice=$currentPrice
    echo "$l,$s,$changeInPrice" >> $t/${s}_2.csv
 done
 rm -f "$t/${s}.csv"
 mv $t/${s}_2.csv $t/${s}.csv
 ) &
 i=$(( $i + 1 ))
 if [ $i -ge 20 ]; then
  i=0
  wait
 fi
done
wait

touch $t/_SUCCESS
hdfs dfs -rm -R -skipTrash "$destinationDir"
hdfs dfs -mkdir -p "$destinationDir" && hdfs dfs -put -f $t/* "$destinationDir"; hdfs dfs -put -f companies_list.txt "$destinationDir"
rm -fr "$t"
echo Saved to $destinationDir
