module purge
module load pandas
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --executor-memory 4g


# read the crime data types into the dataframe
typedf = sqlContext.read.format('com.databricks.spark.csv').options(header = False, inferschema='true').load('types.csv')

header = ['id','CMPLNT_NUM','CMPLNT_FR_DT','CMPLNT_FR_TM','CMPLNT_TO_DT',
        'CMPLNT_TO_TM','RPT_DT','KY_CD','OFNS_DESC','PD_CD','PD_DESC',
        'CRM_ATPT_CPTD_CD','LAW_CAT_CD','JURIS_DESC','BORO_NM',
        'ADDR_PCT_CD','LOC_OF_OCCUR_DESC','PREM_TYP_DESC','PARKS_NM',
        'HADEVELOPT','X_COORD_CD','Y_COORD_CD','Latitude','Longitude','Lat_Lon']

for i in range(25):
   typedf = typedf.withColumnRenamed("C" + str(i), header[i])


# read the crime data into a dataframe
df = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema='true', sep=', ').load('NYPD_Complaint_Data_Historic.csv')


# Create sql tables
sqlContext.registerDataFrameAsTable(df,'crimedata')
sqlContext.registerDataFrameAsTable(typedf,'crimetype')

#########################
#   Base Type Checking  #
#########################

# 1. Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
typedf.groupby('CMPLNT_FR_DT').count().show()
'''
date        |   date between 2006 to 2015
OutDate     |   out of range date, 1900 to 2005, or 2016
WayODate    |   ridiculously out of the date range
Empty       |   empty value
+------------+-------+
|CMPLNT_FR_DT|  count|
+------------+-------+
|       Empty|    655|
|    WayODate|      7|
|        date|5081794|
|     OutDate|  18775|
|         str|      1|
+------------+-------+
'''

# 2. Exact time of occurrence for the reported event (or starting time of occurrence, if CMPLNT_TO_TM exists)
typedf.groupby('CMPLNT_FR_TM').count().show()
'''
time        |   time format as hh:mm:ss
24hTime     |   invalid time format starting with "24:"
Empty       |   empty value
+------------+-------+
|CMPLNT_FR_TM|  count|
+------------+-------+
|        time|5100280|
|       Empty|     48|
|     24hTime|    903|
|         str|      1|
+------------+-------+
'''

# 3. Ending date of occurrence for the reported event, if exact time of occurrence is unknown
typedf.groupby('CMPLNT_TO_DT').count().show()
'''
date        |   date between 2006 to 2015
OutDate     |   out of range date, 1900 to 2005, or 2016
WayODate    |   ridiculously out of the date range
Empty       |   empty value
+------------+-------+
|CMPLNT_TO_DT|  count|
+------------+-------+
|       Empty|1391478|
|    WayODate|      1|
|        date|3704866|
|     OutDate|   4886|
|         str|      1|
+------------+-------+
'''

# 4. Ending time of occurrence for the reported event, if exact time of occurrence is unknown
typedf.groupby('CMPLNT_TO_TM').count().show()
'''
time        |   time format as hh:mm:ss
24hTime     |   invalid time format starting with "24:"
Empty       |   empty value
+------------+-------+
|CMPLNT_TO_TM|  count|
+------------+-------+
|        time|3712070|
|       Empty|1387785|
|     24hTime|   1376|
|         str|      1|
+------------+-------+
'''

# 5. Date event was reported to police
typedf.groupby('RPT_DT').count().show()
'''
date        |   date between 2006 to 2015
+------+-------+
|RPT_DT|  count|
+------+-------+
|  date|5101231|
|   str|      1|
+------+-------+
'''

# 6. Three digit offense classification code
typedf.groupby('KY_CD').count().show()
'''
int         |   3 digits integer
+-----+-------+
|KY_CD|  count|
+-----+-------+
|  str|      1|
|  int|5101231|
+-----+-------+
'''

# 7. Description of offense corresponding with key code
typedf.groupby('OFNS_DESC').count().show()
'''
+---------+-------+
|OFNS_DESC|  count|
+---------+-------+
|      str|5082392|
|    Empty|  18840|
+---------+-------+
'''

df.groupby('OFNS_DESC').count().sort("count", ascending=False).show()
'''
-> shows descriptions of the top 20 offenses corresponding by descending countings
+--------------------+------+
|           OFNS_DESC| count|
+--------------------+------+
|       PETIT LARCENY|822498|
|       HARRASSMENT 2|604070|
|ASSAULT 3 & RELAT...|521538|
|CRIMINAL MISCHIEF...|505774|
|       GRAND LARCENY|429196|
|     DANGEROUS DRUGS|348469|
|OFF. AGNST PUB OR...|283065|
|             ROBBERY|198772|
|            BURGLARY|191406|
|      FELONY ASSAULT|184069|
|   DANGEROUS WEAPONS|124235|
|MISCELLANEOUS PEN...|118721|
|GRAND LARCENY OF ...|102061|
|OFFENSES AGAINST ...|100118|
|INTOXICATED & IMP...| 73730|
|   CRIMINAL TRESPASS| 66544|
|VEHICLE AND TRAFF...| 59140|
|         THEFT-FRAUD| 56762|
|          SEX CRIMES| 54970|
|             FORGERY| 49303|
+--------------------+------+
only showing top 20 rows
'''

# 8. Three digit internal classification code (more granular than Key Code)
typedf.groupby('PD_CD').count().show()
'''
int         |   3 digits integer
Empty       |   empty value
+-----+-------+
|PD_CD|  count|
+-----+-------+
|Empty|   4574|
|  str|      1|
|  int|5096657|
+-----+-------+
'''

# 9. Description of internal classification corresponding with PD code (more granular than Offense Description)
typedf.groupby('PD_DESC').count().show()
'''
str         |   string
Empty       |   empty value
+-------+-------+
|PD_DESC|  count|
+-------+-------+
|  Empty|   4574|
|    str|5096658|
+-------+-------+
'''

df.groupby('PD_DESC').count().sort("count", ascending=False).show()
'''
-> shows descriptions of top 20 internal classifications corresponding with PD code by descending countings
+--------------------+------+
|             PD_DESC| count|
+--------------------+------+
|           ASSAULT 3|438130|
|HARASSMENT,SUBD 3...|368119|
|AGGRAVATED HARASS...|281278|
|HARASSMENT,SUBD 1...|235959|
|LARCENY,PETIT FRO...|221734|
|LARCENY,PETIT FRO...|215446|
|MISCHIEF, CRIMINA...|188807|
|LARCENY,PETIT FRO...|178035|
|MARIJUANA, POSSES...|173044|
|ASSAULT 2,1,UNCLA...|154046|
|CRIMINAL MISCHIEF...|146449|
|LARCENY,GRAND FRO...|121739|
|CONTROLLED SUBSTA...|103085|
|ROBBERY,OPEN AREA...| 99645|
|LARCENY,GRAND OF ...| 91883|
|CRIMINAL MISCHIEF...| 89416|
|BURGLARY,RESIDENC...| 82516|
|LARCENY,PETIT FRO...| 76594|
|WEAPONS, POSSESSI...| 73653|
|INTOXICATED DRIVI...| 73053|
+--------------------+------+
only showing top 20 rows
'''

# 10. Indicator of whether crime was successfully completed or attempted, but failed or was interrupted prematurely
typedf.groupby('CRM_ATPT_CPTD_CD').count().show()
'''
str         |   string
Empty       |   empty value
+----------------+-------+
|CRM_ATPT_CPTD_CD|  count|
+----------------+-------+
|           Empty|      7|
|             str|5101225|
+----------------+-------+
'''

df.groupby('CRM_ATPT_CPTD_CD').count().sort("count", ascending=False).show()
'''
-> shows the indicators of whether crime was completed or attempted
+----------------+-------+
|CRM_ATPT_CPTD_CD|  count|
+----------------+-------+
|       COMPLETED|5013311|
|       ATTEMPTED|  87913|
|                |      7|
+----------------+-------+
'''

# 11. Level of offense: felony, misdemeanor, violation
typedf.groupby('LAW_CAT_CD').count().show()
'''
str         |   string
+----------+-------+
|LAW_CAT_CD|  count|
+----------+-------+
|       str|5101232|
+----------+-------+
'''

df.groupby('LAW_CAT_CD').count().sort("count", ascending=False).show()
'''
-> shows the three levels of different offenses
+-----------+-------+
| LAW_CAT_CD|  count|
+-----------+-------+
|MISDEMEANOR|2918574|
|     FELONY|1567423|
|  VIOLATION| 615234|
+-----------+-------+
'''

# 12. Jurisdiction responsible for incident. Either internal, like Police, Transit, and Housing; or external, like Correction, Port Authority, etc.
typedf.groupby('JURIS_DESC').count().show()
'''
str         |   string
+----------+-------+
|JURIS_DESC|  count|
+----------+-------+
|       str|5101232|
+----------+-------+
'''

df.groupby('JURIS_DESC').count().sort("count", ascending=False).show()
'''
-> shows the top 20 jurisdictions responseble for incident.
+--------------------+-------+
|          JURIS_DESC|  count|
+--------------------+-------+
|    N.Y. POLICE DEPT|4538344|
| N.Y. HOUSING POLICE| 390853|
| N.Y. TRANSIT POLICE| 108817|
|      PORT AUTHORITY|  24657|
|               OTHER|  13575|
|     POLICE DEPT NYC|   8986|
| DEPT OF CORRECTIONS|   4825|
| TRI-BORO BRDG TUNNL|   4633|
|  HEALTH & HOSP CORP|   2590|
|   N.Y. STATE POLICE|   1209|
|         METRO NORTH|    531|
|FIRE DEPT (FIRE M...|    514|
|  LONG ISLAND RAILRD|    439|
|STATN IS RAPID TRANS|    306|
|    N.Y. STATE PARKS|    272|
|    U.S. PARK POLICE|    185|
|             AMTRACK|    153|
|NEW YORK CITY SHE...|    134|
|NYS DEPT TAX AND ...|     77|
|           NYC PARKS|     71|
+--------------------+-------+
only showing top 20 rows
'''

# 13. The name of the borough in which the incident occurred
typedf.groupby('BORO_NM').count().show()
'''
str         |   string
Empty       |   empty value
+-------+-------+
|BORO_NM|  count|
+-------+-------+
|  Empty|    463|
|    str|5100769|
+-------+-------+
'''

df.groupby('BORO_NM').count().sort("count", ascending=False).show()
'''
-> shows the incidents distribution of boroughs
+-------------+-------+
|      BORO_NM|  count|
+-------------+-------+
|     BROOKLYN|1526213|
|    MANHATTAN|1216249|
|        BRONX|1103514|
|       QUEENS|1011002|
|STATEN ISLAND| 243790|
|             |    463|
+-------------+-------+
'''

# 14. The precinct in which the incident occurred
typedf.groupby('ADDR_PCT_CD').count().show()
'''

+-----------+-------+
|ADDR_PCT_CD|  count|
+-----------+-------+
|      Empty|    390|
|        str|      1|
|        int|5100841|
+-----------+-------+
'''

df.groupby('ADDR_PCT_CD').count().sort("count", ascending=False).show(5)
'''
-> shows the top 5 precincts where incidents most frequently occured
+-----------+------+
|ADDR_PCT_CD| count|
+-----------+------+
|         75|165479|
|         43|137771|
|         44|127725|
|         14|119415|
|         40|118896|
+-----------+------+
only showing top 5 rows
'''

df.groupby('ADDR_PCT_CD').count().sort("count").show(5)
'''
-> shows the top 5 precincts where incidents least frequently occured
+-----------+-----+
|ADDR_PCT_CD|count|
+-----------+-----+
|       null|  390|
|         22| 4420|
|        121|17449|
|        100|25879|
|         76|28928|
+-----------+-----+
only showing top 5 rows
'''

# 15. Specific location of occurrence in or around the premises; inside, opposite of, front of, rear of
typedf.groupby('LOC_OF_OCCUR_DESC').count().show()
'''
+-----------------+-------+
|LOC_OF_OCCUR_DESC|  count|
+-----------------+-------+
|            Empty|1127341|
|              str|3973891|
+-----------------+-------+
'''

df.groupby('LOC_OF_OCCUR_DESC').count().sort("count", ascending=False).show()
'''
-> shows the specific location of occurrences in terms of premises
+-----------------+-------+
|LOC_OF_OCCUR_DESC|  count|
+-----------------+-------+
|           INSIDE|2527543|
|         FRONT OF|1189787|
|                 |1127128|
|      OPPOSITE OF| 140606|
|          REAR OF| 113189|
|          OUTSIDE|   2765|
|                 |    213|
+-----------------+-------+
'''

# 16. Specific description of premises; grocery store, residence, street, etc.
typedf.groupby('PREM_TYP_DESC').count().show()
'''
+-------------+-------+
|PREM_TYP_DESC|  count|
+-------------+-------+
|        Empty|  33279|
|          str|5067953|
+-------------+-------+
'''

df.groupby('PREM_TYP_DESC').count().sort("count", ascending=False).show(5)
'''
-> shows the top 5 specific discriptions in terms of premises
+--------------------+-------+
|       PREM_TYP_DESC|  count|
+--------------------+-------+
|              STREET|1697935|
|RESIDENCE - APT. ...|1054419|
|     RESIDENCE-HOUSE| 504402|
|RESIDENCE - PUBLI...| 381745|
|               OTHER| 134762|
+--------------------+-------+
only showing top 5 rows
'''

# 17. Name of NYC park, playground or greenspace of occurrence, if applicable (state parks are not included)
typedf.groupby('PARKS_NM').count().show()
'''
+--------+-------+
|PARKS_NM|  count|
+--------+-------+
|   Empty|5093632|
|     str|   7600|
+--------+-------+
'''

df.groupby('PARKS_NM').count().sort("count", ascending=False).show(10)
'''
-> shows the top 10 common area names in terms of crime occurrences
+--------------------+-------+
|            PARKS_NM|  count|
+--------------------+-------+
|                    |5093632|
|        CENTRAL PARK|    543|
|FLUSHING MEADOWS ...|    301|
|      RIVERSIDE PARK|    188|
|ST. MARY'S PARK B...|    143|
|  MARCUS GARVEY PARK|    137|
|CONEY ISLAND BEAC...|    136|
|      CLAREMONT PARK|    134|
|    MACOMBS DAM PARK|    122|
|  VAN CORTLANDT PARK|    114|
+--------------------+-------+
only showing top 10 rows
'''

# 18. Name of NYCHA housing development of occurrence, if applicable
typedf.groupby('HADEVELOPT').count().show()
'''
+----------+-------+
|HADEVELOPT|  count|
+----------+-------+
|     Empty|4848026|
|       str| 253206|
+----------+-------+
'''

df.groupby('HADEVELOPT').count().sort("count", ascending=False).show(10)
'''
-> shows the top 10 names of NYCHA housing development of occurrence
+-----------+-------+
| HADEVELOPT|  count|
+-----------+-------+
|           |4848026|
|CASTLE HILL|   5495|
| VAN DYKE I|   4585|
|      MARCY|   4298|
|    LINCOLN|   3980|
|     BUTLER|   3960|
|      GRANT|   3886|
|   DOUGLASS|   3749|
|     LINDEN|   3718|
|   FARRAGUT|   3673|
+-----------+-------+
only showing top 10 rows
'''

# 19. X-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)
typedf.groupby('X_COORD_CD').count().show()
'''
+----------+-------+
|X_COORD_CD|  count|
+----------+-------+
|     Empty| 188146|
|       str|      1|
|       int|4913085|
+----------+-------+
'''

df.groupby('X_COORD_CD').count().sort("count", ascending=False).show(5)
'''
-> shows top 5 X-coordinate in terms of crime orrurrences
+----------+------+
|X_COORD_CD| count|
+----------+------+
|      null|188146|
|    987220| 17290|
|    981309|  5863|
|   1016268|  4791|
|   1001575|  4744|
+----------+------+
only showing top 5 rows
'''

# 20. Y-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)
typedf.groupby('Y_COORD_CD').count().show()
'''
+----------+-------+
|Y_COORD_CD|  count|
+----------+-------+
|     Empty| 188146|
|       str|      1|
|       int|4913085|
+----------+-------+
'''

df.groupby('Y_COORD_CD').count().sort("count", ascending=False).show(5)
'''
-> shows top 5 Y-coordinate in terms of crime orrurrences
+----------+------+
|Y_COORD_CD| count|
+----------+------+
|      null|188146|
|    212676| 17353|
|    197980|  5764|
|    227533|  4857|
|    232339|  4731|
+----------+------+
only showing top 5 rows
'''

# 21. Latitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)
typedf.groupby('Latitude').count().show()
'''
+--------+-------+
|Latitude|  count|
+--------+-------+
|   Empty| 188146|
|   float|4913085|
|     str|      1|
+--------+-------+
'''

df.groupby('Latitude').count().sort("count", ascending=False).show(5)
'''
-> shows top 5 latitude coordinates in terms of crime orrurrences
+------------+------+
|    Latitude| count|
+------------+------+
|        null|188146|
|40.750430768| 17232|
|40.710093847|  5710|
|40.791151867|  4779|
|40.804384046|  4731|
+------------+------+
only showing top 5 rows
'''

# 22. Longitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)
typedf.groupby('Longitude').count().show()
'''
+---------+-------+
|Longitude|  count|
+---------+-------+
|    Empty| 188146|
|    float|4913085|
|      str|      1|
+---------+-------+
'''

df.groupby('Longitude').count().sort("count", ascending=False).show(5)
'''
-> shows top 5 longitude coordinates in terms of crime orrurrences
+-------------+------+
|    Longitude| count|
+-------------+------+
|         null|188146|
|-73.989282176| 17232|
| -74.01060963|  5710|
|-73.884371919|  4779|
|-73.937421669|  4731|
+-------------+------+
only showing top 5 rows
'''

# 23. Lat_Lon coordinates for Global Coordinate System, in the format of tuples
typedf.groupby('Lat_Lon').count().show()
'''
+-------+-------+
|Lat_Lon|  count|
+-------+-------+
|  Empty| 188146|
|  tuple|4913085|
|    str|      1|
+-------+-------+
'''

df.groupby('Lat_Lon').count().sort("count", ascending=False).show(5)
'''
-> shows top 5 Lat_Lon coordinates in terms of crime orrurrences
+--------------------+------+
|             Lat_Lon| count|
+--------------------+------+
|                    |188146|
|(40.750430768, -7...| 17232|
|(40.710093847, -7...|  5710|
|(40.791151867, -7...|  4779|
|(40.804384046, -7...|  4731|
+--------------------+------+
only showing top 5 rows
'''


#########################
#   Semantic Checking   #
#########################
'''
1. (pass)
2. Check whether the "Attempted Murder" is correctly catogorized as "Felony Assault";
 - 3. Check whether the "From Date/From Time" is validly before the "To Date/To Time";
 - 4. Check whether the "Report Date" is after both "From Date" and "To Date";
5. Mention that if 4 is violated, check whether the incident killed someone or the death was before the incident ended
6. (pass)
7. Check whether the incident's coordinate cluster to an exact spot. If they do, this is middle of a block;
8. If geo code is NA, check the crime type (rape, sex crime...);
9. same as 7 but police station;
10. pass
11. similar to 7 and 9 but for train
12 (ambitious) Check if geo location in riker island

'''
heck the crime type (rape, sex crime...);
9. same as 7 but police station;
10. pass
11. similar to 7 and 9 but for train
12 (ambitious) Check if geo location in riker island

'''




########################################################################
########################################################################
########################################################################
########################################################################
########################################################################

#time comparison:
module purge
module load pandas
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --executor-memory 4g

typedf = sqlContext.read.format('com.databricks.spark.csv').options(header = False, inferschema='true').load('types.csv')

header = ['id','CMPLNT_NUM','CMPLNT_FR_DT','CMPLNT_FR_TM','CMPLNT_TO_DT',
        'CMPLNT_TO_TM','RPT_DT','KY_CD','OFNS_DESC','PD_CD','PD_DESC',
        'CRM_ATPT_CPTD_CD','LAW_CAT_CD','JURIS_DESC','BORO_NM',
        'ADDR_PCT_CD','LOC_OF_OCCUR_DESC','PREM_TYP_DESC','PARKS_NM',
        'HADEVELOPT','X_COORD_CD','Y_COORD_CD','Latitude','Longitude','Lat_Lon']

for i in range(25):
   typedf = typedf.withColumnRenamed("C" + str(i), header[i])

sqlContext.registerDataFrameAsTable(typedf,'crimetype')



import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import re
from pyspark.sql.functions import unix_timestamp


df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferschema='true',sep=', ').load('NYPD_Complaint_Data_Historic.csv')


def is_date_lv(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/(\d\d\d\d)', string):
        return True
    else:
        return False


def is_time_lv(string):
    if re.findall('(([01]\d)|(2[0-3]))\:([0-5]\d)\:([0-5]\d)', string):
        return True
    else:
        return False


to_date_lv =  udf (lambda x: datetime.strptime(x.strip(), '%m/%d/%Y') if is_date_lv(x) else datetime.strptime('01/01/1001', '%m/%d/%Y'), DateType())

to_time_lv = udf(lambda x: datetime.strptime(x.strip(), '%H:%M:%S') if is_time_lv(x) else datetime.strptime('01:01:01', '%H:%M:%S'), DateType())

df = df.withColumn('CMPLNT_TO_DT', to_date_lv(col('CMPLNT_TO_DT')))
df = df.withColumn('CMPLNT_FR_DT', to_date_lv(col('CMPLNT_FR_DT')))
df = df.withColumn('CMPLNT_TO_TM', to_time_lv(col('CMPLNT_TO_TM')))
df = df.withColumn('CMPLNT_FR_TM', to_time_lv(col('CMPLNT_FR_TM')))
df = df.withColumn('RPT_DT', to_date_lv(col('RPT_DT')))

sqlContext.registerDataFrameAsTable(df,'crimedata')

pdf = df.select(['Latitude','Longitude']).toPandas()
pdf.dropna(inplace=True)
pdf['LatLon'] = pdf['Latitude'].round(5).map(str) + pdf['Longitude'].round(5).map(str)

val_cnts = pdf['LatLon'].value_counts()
val_cnts
'''
40.75043-73.98928    17232
40.71009-74.01061     5710
40.79115-73.88437     4779
40.80438-73.93742     4731
40.75627-73.9905      4575
40.61072-73.92099     4293
40.74912-73.98617     4269
40.73393-73.87158     4245
40.68446-73.97775     4143
40.6517-73.86845      4053
40.804-73.87834       3785
40.82988-73.93676     3760
40.73449-73.86801     3407
40.80837-73.94689     3331
40.75664-73.98837     3243
40.69087-73.98585     3167
40.85214-73.92238     3034
40.87367-73.90801     3026
40.75724-73.98979     3011
40.76187-73.96636     2993
40.68336-73.97487     2861
40.81113-73.93671     2757
40.75987-73.82897     2688
40.73512-73.99146     2679
40.78872-73.94        2652
40.62215-74.02728     2529
40.86847-73.82158     2499
40.72364-73.9983      2494
40.80405-73.93662     2414
40.74973-73.98968     2374
'''
tup_df = df.select(['Latitude','Longitude']).toPandas()
tup_df2 = df.select(['Lat_Lon']).toPandas()
tup_df2[['Lat_est', 'Lon_est']] = tup_df2['Lat_Lon'].str[1:-1].str.split(',', expand=True)
tup_df2['Lat_est'] = pd.to_numeric(tup_df2['Lat_est'])
tup_df2['Lon_est'] = pd.to_numeric(tup_df2['Lon_est'])
tup_df2['Lat_raw'] = tup_df['Latitude']
tup_df2['Lon_raw'] = tup_df['Longitude']
tup_df2['Lat_diff'] = tup_df2['Lat_raw'] - tup_df2['Lat_est']
tup_df2['Lat_diff'] = tup_df2['Lat_diff'].abs()
tup_df2['Lon_diff'] = tup_df2['Lon_raw'] - tup_df2['Lon_est'] 
tup_df2['Lon_diff'] = tup_df2['Lon_diff'].abs()

problems = (tup_df2['Lat_diff'] > .00001) | (tup_df2['Lon_diff'] > .00001)
tup_df2['Lat_diff'].max().
#7.1054273576010019e-15
tup_df2['Lon_diff'].max()
#1.4210854715202004e-14
#NO PROBLEMS! FML

#if geo code n/a, check offense type.

typedf.groupby('Latitude').count().show()
sqlContext.sql("SELECT crimedata.OFNS_DESC, count(*) as cnt FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.Latitude = 'Empty' or crimetype.Longitude = 'Empty' group by crimedata.OFNS_DESC order by cnt desc").show(500)

'''
+--------------------+-----+                                                    
|           OFNS_DESC|  cnt|
+--------------------+-----+
|          SEX CRIMES|54932|
|       PETIT LARCENY|20932|
|                RAPE|13791|
|       GRAND LARCENY|12780|
|CRIMINAL MISCHIEF...|11253|
|ASSAULT 3 & RELAT...| 8952|
|       HARRASSMENT 2| 8592|
|     DANGEROUS DRUGS| 7549|
|VEHICLE AND TRAFF...| 7111|
|INTOXICATED & IMP...| 6339|
|             ROBBERY| 5791|
|   DANGEROUS WEAPONS| 4043|
|      FELONY ASSAULT| 3746|
|OFF. AGNST PUB OR...| 3397|
|GRAND LARCENY OF ...| 3034|
|MISCELLANEOUS PEN...| 2697|
|            BURGLARY| 2387|
|OFFENSES AGAINST ...| 1945|
|   CRIMINAL TRESPASS| 1599|
|             FORGERY| 1311|
|                    |  740|
|              FRAUDS|  715|
|         THEFT-FRAUD|  692|
|POSSESSION OF STO...|  690|
|UNAUTHORIZED USE ...|  566|
|OFFENSES AGAINST ...|  496|
|OTHER OFFENSES RE...|  290|
|NYS LAWS-UNCLASSI...|  285|
|OFFENSES INVOLVIN...|  283|
|               ARSON|  231|
| ADMINISTRATIVE CODE|  212|
|OTHER STATE LAWS ...|  168|
|   THEFT OF SERVICES|   98|
|FRAUDULENT ACCOSTING|   91|
|PROSTITUTION & RE...|   88|
|     BURGLAR'S TOOLS|   55|
|KIDNAPPING & RELA...|   45|
|  DISORDERLY CONDUCT|   41|
|OFFENSES AGAINST ...|   32|
|PETIT LARCENY OF ...|   31|
|            GAMBLING|   16|
|            JOSTLING|   14|
|OFFENSES RELATED ...|   14|
|HOMICIDE-NEGLIGEN...|   10|
|    OTHER STATE LAWS|    9|
|INTOXICATED/IMPAI...|    8|
|CHILD ABANDONMENT...|    8|
|           LOITERING|    7|
|LOITERING/GAMBLIN...|    7|
|ALCOHOLIC BEVERAG...|    6|
|            ESCAPE 3|    5|
|OTHER TRAFFIC INF...|    3|
|ENDAN WELFARE INCOMP|    3|
|HOMICIDE-NEGLIGEN...|    3|
|MURDER & NON-NEGL...|    1|
|ANTICIPATORY OFFE...|    1|
|NEW YORK CITY HEA...|    1|
+--------------------+-----+
'''

#check for geo location outliers

pdf = df.select(['Latitude','Longitude']).toPandas()


sqlContext.sql("SELECT crimedata.CMPLNT_TO_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_TO_DT = 'WayODate'").show(10)
'''
+------------+                                                                  
|CMPLNT_TO_DT|
+------------+
|  2090-04-06|
+------------+
'''

sqlContext.sql("SELECT count(*) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'WayODate'").show(10)




sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT, crimedata.CMPLNT_TO_DT, crimedata.CMPLNT_FR_TM, crimedata.CMPLNT_TO_TM FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimetype.CMPLNT_TO_TM = 'time' and crimetype.CMPLNT_FR_TM = 'time' and crimedata.CMPLNT_TO_DT = crimedata.CMPLNT_FR_DT and crimedata.CMPLNT_TO_TM < crimedata.CMPLNT_FR_TM").show(30)

'''
+------------+------------+------------+------------+                           
|CMPLNT_FR_DT|CMPLNT_TO_DT|CMPLNT_FR_TM|CMPLNT_TO_TM|
+------------+------------+------------+------------+
|  06/22/2014|  06/22/2014|    23:00:00|    00:00:00|
+------------+------------+------------+------------+
'''


sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT, crimedata.CMPLNT_TO_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT < crimedata.CMPLNT_FR_DT ").show(30)

'''
+------------+------------+                                                     
|CMPLNT_FR_DT|CMPLNT_TO_DT|
+------------+------------+
|  2015-07-10|  2015-05-10|
|  2013-01-31|  2013-01-29|
|  2015-05-15|  2015-05-09|
|  2009-06-03|  2008-12-17|
|  2011-04-23|  2010-03-01|
|  2010-03-24|  2010-03-23|
|  2008-09-07|  2008-04-08|
|  2012-08-09|  2012-05-09|
|  2015-09-12|  2015-09-03|
|  2015-05-16|  2015-05-15|
|  2014-01-18|  2014-01-08|
|  2009-11-10|  2009-11-09|
|  2012-04-07|  2011-04-07|
|  2006-05-19|  2006-05-14|
|  2014-05-03|  2013-05-03|
|  2012-01-26|  2012-01-24|
|  2011-07-19|  2011-07-14|
|  2009-01-17|  2009-01-13|
|  2011-05-23|  2011-05-08|
|  2012-07-31|  2012-07-13|
|  2009-11-15|  2009-11-14|
|  2008-01-18|  2008-01-08|
|  2015-05-21|  2015-05-17|
|  2015-05-07|  2015-05-05|
|  2013-06-11|  2013-06-09|
|  2011-08-02|  2011-08-01|
|  2011-04-28|  2010-04-28|
|  2010-11-11|  2010-11-10|
|  2010-03-14|  2009-11-14|
|  2015-09-24|  2015-09-19|
+------------+------------+
'''


sqlContext.sql("SELECT count(*) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.RPT_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT > crimedata.RPT_DT ").show(30)
'''
+----+                                                                          
| _c0|
+----+
|4131|
+----+
'''
sqlContext.sql("SELECT crimedata.CMPLNT_TO_DT, crimedata.RPT_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.RPT_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT > crimedata.RPT_DT ").show(30)
'''
too many to look into
'''


sqlContext.sql("SELECT count(*) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.RPT_DT = 'date' and crimetype.CMPLNT_FR_DT = 'date' and crimedata.CMPLNT_FR_DT > crimedata.RPT_DT ").show(30)
'''
+---+                                                                           
|_c0|
+---+
|  2|
+---+
'''
sqlContext.sql("SELECT crimedata.* FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.RPT_DT = 'date' and crimetype.CMPLNT_FR_DT = 'date' and crimedata.CMPLNT_FR_DT > crimedata.RPT_DT ").show(30)
'''
both petty larsony but with date quoted at 1900.
+----------+------------+------------+------------+------------+----------+-----+-------------+-----+--------------------+----------------+-----------+----------------+---------+-----------+-----------------+----------------+--------+----------+----------+----------+------------+-------------+--------------------+
|CMPLNT_NUM|CMPLNT_FR_DT|CMPLNT_FR_TM|CMPLNT_TO_DT|CMPLNT_TO_TM|    RPT_DT|KY_CD|    OFNS_DESC|PD_CD|             PD_DESC|CRM_ATPT_CPTD_CD| LAW_CAT_CD|      JURIS_DESC|  BORO_NM|ADDR_PCT_CD|LOC_OF_OCCUR_DESC|   PREM_TYP_DESC|PARKS_NM|HADEVELOPT|X_COORD_CD|Y_COORD_CD|    Latitude|    Longitude|             Lat_Lon|
+----------+------------+------------+------------+------------+----------+-----+-------------+-----+--------------------+----------------+-----------+----------------+---------+-----------+-----------------+----------------+--------+----------+----------+----------+------------+-------------+--------------------+
| 560100536|  2015-08-15|  1900-01-01|  2015-08-15|  1900-01-01|2015-01-05|  341|PETIT LARCENY|  333|LARCENY,PETIT FRO...|       COMPLETED|MISDEMEANOR|N.Y. POLICE DEPT|MANHATTAN|          1|           INSIDE|DEPARTMENT STORE|        |          |    984112|    203624|40.725585762|-74.000499329|(40.725585762, -7...|
| 551812699|  2015-08-30|  1900-01-01|  2015-08-30|  1900-01-01|2015-01-01|  341|PETIT LARCENY|  338|LARCENY,PETIT FRO...|       COMPLETED|MISDEMEANOR|N.Y. POLICE DEPT| BROOKLYN|         75|           INSIDE|    LIQUOR STORE|        |          |   1017697|    184972|40.674327192|-73.879422815|(40.674327192, -7...|
+----------+------------+------------+------------+------------+----------+-----+-------------+-----+--------------------+----------------+-----------+----------------+---------+-----------+-----------------+----------------+--------+----------+----------+----------+------------+-------------+--------------------+
'''


sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT, crimedata.RPT_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.RPT_DT = 'date' and crimetype.CMPLNT_FR_DT = 'date' and crimedata.CMPLNT_FR_DT > crimedata.RPT_DT ").show(30)
'''
+------------+----------+                                                       
|CMPLNT_FR_DT|    RPT_DT|
+------------+----------+
|  2015-08-15|2015-01-05|
|  2015-08-30|2015-01-01|
+------------+----------+
'''

OFNS_DESC
 or crimedata.OFNS_DESC = 'ATTEMPTED MURDER'
sqlContext.sql("SELECT count(*) FROM crimedata where crimedata.OFNS_DESC = 'FELONY ASSAULT'").show()
'''
+------+                                                                        
|   _c0|
+------+
|184069|
+------+
'''
sqlContext.sql("SELECT count(*) FROM crimedata where crimedata.OFNS_DESC = 'ATTEMPTED MURDER'").show()
'''
+---+                                                                           
|_c0|
+---+
|  0|
+---+
'''

#########################
#   Semantic Checking   #
#########################
'''
1. (pass)
2. Check whether the "Attempted Murder" is correctly catogorized as "Felony Assault";
 - 3. Check whether the "From Date/From Time" is validly before the "To Date/To Time";
 - 4. Check whether the "Report Date" is after both "From Date" and "To Date";
5. Mention that if 4 is violated, check whether the incident killed someone or the death was before the incident ended
6. (pass)
7. Check whether the incident's coordinate cluster to an exact spot. If they do, this is middle of a block;
8. If geo code is NA, check the crime type (rape, sex crime...);
9. same as 7 but police station;
10. pass
11. similar to 7 and 9 but for train
12 (ambitious) Check if geo location in riker island

'''











































####NOT REALLY IMPORTANT:
sqlContext.sql("SELECT count(*) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT < crimedata.CMPLNT_FR_DT ").show(10)
'''
+---+                                                                           
|_c0|
+---+
| 30|
+---+
'''
sqlContext.sql("SELECT count(*) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT > crimedata.CMPLNT_FR_DT ").show(10)
'''
+------+                                                                        
|   _c0|
+------+
|761560|
+------+
'''






sqlContext.sql("SELECT crimedata.CMPLNT_TO_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_TO_DT = 'date'").show(10)

sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date'").show(10)


sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT, crimedata.CMPLNT_TO_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT > crimedata.CMPLNT_FR_DT ").show(10)



sqlContext.sql("SELECT crimedata.CMPLNT_FR_DT, crimedata.CMPLNT_TO_DT FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date' and crimetype.CMPLNT_TO_DT = 'date' and crimedata.CMPLNT_TO_DT < crimedata.CMPLNT_FR_DT ").show(10)

















################TRASHBIN


sqlContext.sql("SELECT convert(crimedata.CMPLNT_FR_DT,date, 101) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date'").show(10)

sqlContext.sql("SELECT left(crimedata.CMPLNT_FR_DT,1) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date'").show(10)



sqlContext.sql("SELECT substr(crimedata.CMPLNT_FR_DT,1,1) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date'").show(10)



sqlContext.sql("SELECT cast(crimedata.CMPLNT_TO_DT as date) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_TO_DT = 'date'").show(10)



sqlContext.sql("SELECT cast(crimedata.CMPLNT_FR_DT as date) FROM crimetype inner join crimedata on crimedata.CMPLNT_NUM = crimetype.id where crimetype.CMPLNT_FR_DT = 'date'").show(10)




