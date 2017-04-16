
# BIGDATA Project 

##Lee and Viola

### Dataset: 
[NYC Crime](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

### Put your dataset onto hadoop, run:  

 ```
 hadoop fs -copyFromLocal NYPD_Complaint_Data_Historic.csv
 ```

### To get the feature analysis outputs, run:  

 ```
 bash run_all_scripts.sh
 ```

### To explore more about the time-series data analysis:

 > Use PysparkSQL to explore the crime type analysis with respect to year, month and day of week. Below is an example:
 
  ```
  kycd_y_m_d_mapreduce.py
  ```

 > Use MapReduce to explore the crime type analysis with respect to hour. Below is an example:

  ```
  kycd_hour_mapreduce.py
  ```
  
 > All the plots scripts and notebooks are in the folder "plots"
