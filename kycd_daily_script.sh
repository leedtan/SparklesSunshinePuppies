OUT="kycd_daily.csv"
PY="kycd_daily_mapreduce.py"
FILE="NYPD_Complaint_Data_Historic.csv"

hadoop fs -rm -r "$OUT"
spark-submit "$PY" "$FILE"
hadoop fs -getmerge "$OUT" "$OUT"