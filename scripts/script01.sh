out="CMPLNT_FR_DT.out"
py="../feature_mapreduce/mapreduce1.py"
file="NYPD_Complaint_Data_Historic.csv"
hadoop fs -rm -r "$out"
spark-submit "$py" "$file"
hadoop fs -getmerge "$out" "$out"






