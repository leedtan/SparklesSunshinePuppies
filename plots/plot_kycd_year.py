import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
sns.set_style("whitegrid")


col_names = ['year', 'KY_CD', 'cnt']
df = pd.read_csv('year_valid.csv', header=None, names=col_names, dtype=int)

ttl = df.groupby(df.year).sum().cnt.values
year = range(2006, 2016)

# Select the top 6 offense classification types into the pool
pool = [341, 578, 344, 351, 109, 235]
pool_desc = ['Petit Larceny', 'Harrassment 2', 'Assault 3', 'Criminal Mischief', 'Grand Larceny', 'Dangerous Drugs']
df_tops = df[df.KY_CD.isin(pool)]

lst = []
for i in pool:
    lst.append(df_tops[df_tops.KY_CD==i].cnt.values)


# KY_CD bar chart with respect to years
 trend over year
plt.figure(figsize=(15, 6))
c = sns.color_palette("muted", 6)
c2 = sns.color_palette("bright", 6)

N = len(year)
ind = np.arange(N)
w = 0.15

for i in range(6):
    plt.bar(ind+w*i, lst[i], w, color=c[i], label=pool_desc[i], alpha=0.3)
for i in range(6):
    plt.plot(ind+w*(i+.5), lst[i], "o-", color=c2[i], label=pool_desc[i], linewidth=3)

plt.xlabel('Year')
plt.ylabel('Crime Occurrence')
plt.title('Top-6 Crime Type Over Year', fontsize=16)
plt.xticks(ind+3*w, year)
plt.legend(bbox_to_anchor=(1.03, 0.8), loc=2, borderaxespad=0.)
plt.tight_layout()
plt.savefig('kycd_year_bar.png')
plt.show()


# Total crime occurrences with respect to years
plt.figure(figsize=(12, 4))
plt.plot(year, ttl, "o-", linewidth=3, fillstyle='full', color='skyblue')
plt.xticks(year, [str(x) for x in range(2006, 2016)])
plt.title('Total Crime Occurrence Over Year')
plt.xlabel('Year')
plt.ylabel('Crime Occurrence')
for i in range(len(ttl)):
    plt.text(year[i]-0.3, ttl[i]+2000, str(ttl[i]), fontsize=12, color='steelblue')
plt.fill_between(year, ttl, 340000, alpha=0.3, color='skyblue')
plt.tight_layout()
plt.show()
plt.savefig('ttl_year.png')



# Select the top 10 offense classification types into the pool
pool_10 = [341, 578, 344, 351, 109, 235, 361, 105, 107, 106]
pool_desc_10 = ['Petit Larceny', 'Harrassment 2', 'Assault 3', 'Criminal Mischief', 'Grand Larceny', 'Dangerous Drugs',
               'Offenses Against Public Order', 'Robbery', 'Burglary', 'Felony Assault']
df_tops_10 = df[df.KY_CD.isin(pool_10)]


lst = []
for i in pool_10:
    lst.append(df_tops_10[df_tops_10.KY_CD==i].cnt.values)

# KY_CD line chart with respect to years
plt.figure(figsize=(12, 6))
c3 = sns.cubehelix_palette(10, dark=0.35, light=0.9, reverse=True)
for i in range(10):
    plt.plot(year, lst[i], "o-", label=pool_desc_10[i], color=c3[i], linewidth=3)
plt.xticks(year, [str(x) for x in range(2006, 2016)])
plt.xlabel('Year')
plt.ylabel('Crime Occurrence')
plt.title('Top 10 Crime Type Over Year')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.savefig('kycd_year_line.png')
plt.show()




