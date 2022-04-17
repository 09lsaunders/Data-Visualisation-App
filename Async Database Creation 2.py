
import pandas as pd

import sqlite3
import numpy as np



dfemsnov18 = pd.read_csv('/Users/Luke/ENGR301/For Cloud and GitHub/ems-nov-2018.csv')


dfemsaug18oct18test = pd.concat([dfemsnov18])

df2emsaug18oct18test = dfemsaug18oct18test.loc[:, ['timestamp','metadata_id', 'consumption', 'meter_reading']]

X = df2emsaug18oct18test.loc[:, ['metadata_id']]

emsY = X.metadata_id.unique()


print(emsY)

emstables = []


df3emsaug18oct18test = df2emsaug18oct18test.sort_values(by=['timestamp'])

df3emsaug18oct18test = df3emsaug18oct18test.loc[0:2000, :]


conn = sqlite3.connect('EMSOUT.db')

a = 0

while a < len(emsY):
    df4emsaug18oct18test = df3emsaug18oct18test[df3emsaug18oct18test['metadata_id'] == emsY[a]]

    if np.isnan(emsY[a]):
        table = 'EMStest' + str(emsY[a])

    else:
        table = 'EMStest' + str(int(emsY[a]))
        
    print(table)
    emstables.append(table)

    dfems5 = df4emsaug18oct18test

    dfems5.to_sql(table, conn, if_exists='replace', index=False)




    a+=1



conn = sqlite3.connect('EMSOUT.db')

c = conn.cursor()

query2 = "SELECT `timestamp` FROM 'EMStest101'"

c.execute(query2)

timeaxis = c.fetchall()
timeaxis2 = np.array(timeaxis)


print(timeaxis2)


conn.close()









dfsynnov18 = pd.read_csv('/Users/Luke/ENGR301/For Cloud and GitHub/synetica-nov-2018.csv')



dfsynaug18oct18test = pd.concat([dfsynnov18])

df2synaug18oct18test = dfsynaug18oct18test.loc[:, ['timestamp','name', 'reading']]

X2 = df2synaug18oct18test.loc[:, ['name']]

synY2 = X2.name.unique()

print(synY2)

syntables = []
###########

df3synaug18oct18test = df2synaug18oct18test.sort_values(by=['timestamp'])

df3synaug18oct18test = df3synaug18oct18test.loc[0:1000, :]

conn = sqlite3.connect('SynOUT.db')

a = 0

while a < len(synY2):
    df4synaug18oct18test = df3synaug18oct18test[df3synaug18oct18test['name'] == synY2[a]]

    table = 'Syntest' + str(synY2[a])
    print(table)
    syntables.append(table)

    dfsyn5 = df4synaug18oct18test

    dfsyn5.to_sql(table, conn, if_exists='replace', index=False)




    a+=1

#############

conn = sqlite3.connect('SynOUT.db')

c = conn.cursor()

query2 = "SELECT `timestamp` FROM 'SyntestMC044-L01/M3'"

c.execute(query2)

timeaxis = c.fetchall()
timeaxis2 = np.array(timeaxis)


print(timeaxis2)

conn.close()




dfwifinov18 = pd.read_csv('/Users/Luke/ENGR301/For Cloud and GitHub/wifi_2018-11.csv')

print(dfwifinov18)


dfwifiaug18feb19test = pd.concat([dfwifinov18])

df2wifiaug18feb19test = dfwifiaug18feb19test.loc[:, ['Building','Floor', 'time','Associated Client Count', 'Authenticated Client Count']]



df8 = df2wifiaug18feb19test.loc[:, ['Building', 'Floor']]

X3 = df8


Y4 = df8.groupby(['Building','Floor']).size().reset_index()

Y5 = Y4[['Building','Floor']]

wifiY6 = np.array(Y5)

print(wifiY6)

wifitables = []
wifitablename = []


df3wifiaug18feb19test = df2wifiaug18feb19test.sort_values(by=['time', 'Building', 'Floor'])

df3wifiaug18feb19test = df3wifiaug18feb19test.loc[0:30000, :]



conn = sqlite3.connect('test2.db')

a = 0

while a < len(wifiY6):

    Building_name = wifiY6[a][0]
    Floor_name = wifiY6[a][1]

    Building_name = str(Building_name).strip()
    Floor_name = str(Floor_name).strip()

    df4wifiaug18feb19test = df3wifiaug18feb19test[df3wifiaug18feb19test['Building'].str.contains(Building_name)]

    df5wifiaug18feb19test = df4wifiaug18feb19test[df4wifiaug18feb19test['Floor'].str.contains(Floor_name)]


    table = 'wifitest' + Building_name.upper() + Floor_name.upper()
    tablename = 'wifitest' + Building_name.upper() + '|' + Floor_name.upper()
    print(table)

    Building_name2 = wifiY6[a-1][0]
    Floor_name2 = wifiY6[a-1][1]

    Building_name2 = str(Building_name2).strip()
    Floor_name2 = str(Floor_name2).strip()

    A = Building_name.upper() + Floor_name.upper()
    B = Building_name2.upper() + Floor_name2.upper()

    if A == B:
        print(A)
        print(B)
    else:
        wifitables.append(table)
        wifitablename.append(tablename)

    dfwifi5 = df5wifiaug18feb19test

    dfwifi5.to_sql(table, conn, if_exists='replace', index=False)




    a+=1


conn = sqlite3.connect('test2.db')

c = conn.cursor()

query2 = "SELECT `time` FROM 'wifitestAlex SquareCosta'"

c.execute(query2)

timeaxis = c.fetchall()
timeaxis2 = np.array(timeaxis)


print(timeaxis2)

print(emstables)
print(syntables)
print(wifitables)
print(wifitablename)

conn.close()

EMStablesdf = pd.DataFrame({'tables':emstables})
Syntablesdf = pd.DataFrame({'tables':syntables})
Wifitablesdf = pd.DataFrame({'tables':wifitables})

Wifitablesnamesdf = pd.DataFrame({'tables':wifitablename})


EMStablesdf.to_csv("/Users/Luke/ENGR301/For Cloud and GitHub/EMStables.csv")
Syntablesdf.to_csv("/Users/Luke/ENGR301/For Cloud and GitHub/Syntables.csv")
Wifitablesnamesdf.to_csv("/Users/Luke/ENGR301/For Cloud and GitHub/Wifitables.csv")



