import duckdb 
import os 
# Assuming uniform population distribution accross the lga's 
def load_csv(file):
    pass
    

TRIPS = duckdb.read_csv(os.path.join(os.getcwd(), 'data', 'T_VISTA1218_V1.csv'))
PERSONS = duckdb.read_csv(os.path.join(os.getcwd(), 'data', 'P_VISTA1218_V2.csv'))
HOMES = duckdb.read_csv(os.path.join(os.getcwd(), 'data', 'H_VISTA_1218_V1.csv'))
test = duckdb.sql("select (select HOMELGA from HOMES h where h.HHID = t.HHID) , sum(CUMDIST) from TRIPS t group by (select HOMELGA from HOMES h where h.HHID = t.HHID) limit 5")


cumulative_person_travel = duckdb.sql("select t.HHID, t.PERSID, sum(t.CUMDIST) as TOTAL_DISTANCE from TRIPS t group by t.PERSID, t.HHID order by t.persid")

# Add home LGA to cumulative person travel
lga_person_dist =  duckdb.sql ( """
                               SELECT CASE 
                               WHEN REGEXP_MATCH(h.homelga, '\(C\)') THEN REGEXP_REPLACE(h.HOMELGA, '\(C\)', 'City') 
                               WHEN REGEXP_MATCH(h.homelga, '\(C\)') THEN REGEXP_REPLACE(h.HOMELGA, '\(S\)', 'Shire') AS homelga,  
                               cpt.* FROM cumulative_person_travel cpt 
                               JOIN homes h ON cpt.hhid = h.hhid
                               """)
print(duckdb.sql("Select * from lga_person_dist where homelga like '%City%'"))
# print(cumulative_person_travel)
# print(lga_person_dist)
median = duckdb.sql("Select HOMELGA, round(median(TOTAL_DISTANCE),2) from  lga_person_dist group by homelga")





# print(test)