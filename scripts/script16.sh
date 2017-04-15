out="PREM_TYP_DESC.out"
py="../feature_mapreduce/mapreduce16.py"
file="NYPD_Complaint_Data_Historic.csv"
hadoop fs -rm -r "$out"
spark-submit "$py" "$file"
hadoop fs -getmerge "$out" "$out"






