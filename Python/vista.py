import duckdb 
import os 
# Assuming uniform population distribution accross the lga's 
def write_csv(self, name):
    self.to_csv(os.path.join(os.getcwd(),'output_data',name), header = True )
duckdb.DuckDBPyRelation.write_csv = write_csv

TRIPS = duckdb.read_csv(os.path.join(os.getcwd(), 'external_data', 'T_VISTA1218_V1.csv'), na_values='N/A')
PERSONS = duckdb.read_csv(os.path.join(os.getcwd(), 'external_data', 'P_VISTA1218_V2.csv'), na_values='N/A')
HOMES = duckdb.read_csv(os.path.join(os.getcwd(), 'external_data', 'H_VISTA_1218_V1.csv'), na_values='N/A')
test = duckdb.sql("select (select HOMELGA from HOMES h where h.HHID = t.HHID) , sum(CUMDIST) from TRIPS t group by (select HOMELGA from HOMES h where h.HHID = t.HHID) limit 5")


# CAR_TRAVEL = duckdb.sql(""" SELECT t.TRIPID, t.PERSID, t.HHID,
#                         CASE WHEN MODE1='Vehicle Driver' THEN DIST1 ELSE 0 END AS DIST1,
#                         CASE WHEN MODE2='Vehicle Driver' THEN DIST2 ELSE 0 END AS DIST2,
#                         CASE WHEN MODE3='Vehicle Driver' THEN DIST3 ELSE 0 END AS DIST3,
#                         CASE WHEN MODE4='Vehicle Driver' THEN DIST4 ELSE 0 END AS DIST4,
#                         CASE WHEN MODE5='Vehicle Driver' THEN DIST5 ELSE 0 END AS DIST5,
#                         CASE WHEN MODE6='Vehicle Driver' THEN DIST6 ELSE 0 END AS DIST6,
#                         CASE WHEN MODE7='Vehicle Driver' THEN DIST7 ELSE 0 END AS DIST7,
#                         CASE WHEN MODE8='Vehicle Driver' THEN DIST8 ELSE 0 END AS DIST8,
#                         CASE WHEN MODE9='Vehicle Driver' THEN DIST9 ELSE 0 END AS DIST9,
#                         CASE WHEN MODE1='Vehicle Driver' THEN TIME1 ELSE 0 END AS TIME1,
#                         CASE WHEN MODE2='Vehicle Driver' THEN TIME2 ELSE 0 END AS TIME2,
#                         CASE WHEN MODE3='Vehicle Driver' THEN TIME3 ELSE 0 END AS TIME3,
#                         CASE WHEN MODE4='Vehicle Driver' THEN TIME4 ELSE 0 END AS TIME4,
#                         CASE WHEN MODE5='Vehicle Driver' THEN TIME5 ELSE 0 END AS TIME5,
#                         CASE WHEN MODE6='Vehicle Driver' THEN TIME6 ELSE 0 END AS TIME6,
#                         CASE WHEN MODE7='Vehicle Driver' THEN TIME7 ELSE 0 END AS TIME7,
#                         CASE WHEN MODE8='Vehicle Driver' THEN TIME8 ELSE 0 END AS TIME8,
#                         CASE WHEN MODE9='Vehicle Driver' THEN TIME9 ELSE 0 END AS TIME9
#                         from TRIPS t
#                         """)
TRIP_MODE_DISTANCE = duckdb.sql(""" SELECT t.TRIPID, t.PERSID, t.HHID,
                        CASE WHEN MODE1='Vehicle Driver' THEN CAST(DIST1 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE2='Vehicle Driver' THEN CAST(DIST2 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE3='Vehicle Driver' THEN CAST(DIST3 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE4='Vehicle Driver' THEN CAST(DIST4 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE5='Vehicle Driver' THEN CAST(DIST5 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE6='Vehicle Driver' THEN CAST(DIST6 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE7='Vehicle Driver' THEN CAST(DIST7 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE8='Vehicle Driver' THEN CAST(DIST8 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE9='Vehicle Driver' THEN CAST(DIST9 AS DOUBLE) ELSE 0 END AS DRIVEN_DISTANCE, 
                        CASE WHEN MODE1='Bicycle' THEN CAST(DIST1 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE2='Bicycle' THEN CAST(DIST2 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE3='Bicycle' THEN CAST(DIST3 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE4='Bicycle' THEN CAST(DIST4 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE5='Bicycle' THEN CAST(DIST5 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE6='Bicycle' THEN CAST(DIST6 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE7='Bicycle' THEN CAST(DIST7 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE8='Bicycle' THEN CAST(DIST8 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE9='Bicycle' THEN CAST(DIST9 AS DOUBLE) ELSE 0 END AS BICYCLED_DISTANCE, 
                        CASE WHEN MODE1='Train' THEN CAST(DIST1 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE2='Train' THEN CAST(DIST2 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE3='Train' THEN CAST(DIST3 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE4='Train' THEN CAST(DIST4 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE5='Train' THEN CAST(DIST5 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE6='Train' THEN CAST(DIST6 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE7='Train' THEN CAST(DIST7 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE8='Train' THEN CAST(DIST8 AS DOUBLE) ELSE 0 END + 
                        CASE WHEN MODE9='Train' THEN CAST(DIST9 AS DOUBLE) ELSE 0 END AS TRAINED_DISTANCE, 
                        from TRIPS t""")

                        # CASE WHEN MODE1='Vehicle Driver' THEN TIME1 ELSE 0 END AS TIME1,
                        # CASE WHEN MODE2='Vehicle Driver' THEN TIME2 ELSE 0 END AS TIME2,
                        # CASE WHEN MODE3='Vehicle Driver' THEN TIME3 ELSE 0 END AS TIME3,
                        # CASE WHEN MODE4='Vehicle Driver' THEN TIME4 ELSE 0 END AS TIME4,
                        # CASE WHEN MODE5='Vehicle Driver' THEN TIME5 ELSE 0 END AS TIME5,
                        # CASE WHEN MODE6='Vehicle Driver' THEN TIME6 ELSE 0 END AS TIME6,
                        # CASE WHEN MODE7='Vehicle Driver' THEN TIME7 ELSE 0 END AS TIME7,
                        # CASE WHEN MODE8='Vehicle Driver' THEN TIME8 ELSE 0 END AS TIME8,
                        # CASE WHEN MODE9='Vehicle Driver' THEN TIME9 ELSE 0 END AS TIME9

print(TRIP_MODE_DISTANCE)

PERSON_CULM_TRIP_MODE_DISTANCE = duckdb.sql("""
                                            select tmd.HHID, tmd.PERSID, 
                                            round(sum(DRIVEN_DISTANCE), 3) as P_DRIVEN_DISTANCE, 
                                            round(sum(BICYCLED_DISTANCE), 3) as P_BICYCLED_DISTANCE, 
                                            round(sum(TRAINED_DISTANCE), 3) as P_TRAINED_DISTANCE, 
                                            from TRIP_MODE_DISTANCE tmd
                                            group by tmd.PERSID, tmd.HHID order by tmd.persid
                                            """)
print(PERSON_CULM_TRIP_MODE_DISTANCE)

# Add home LGA to cumulative person travel
lga_person_dist =  duckdb.sql ( """ SELECT CASE 
                                WHEN REGEXP_MATCHES(h.homelga, '\(C\)') THEN REGEXP_REPLACE(h.HOMELGA, '\(C\)', 'City') 
                                WHEN REGEXP_MATCHES(h.homelga, '\(S\)') THEN REGEXP_REPLACE(h.HOMELGA, '\(S\)', 'Shire')
                                END AS homelga,  
                                cpt.* FROM PERSON_CULM_TRIP_MODE_DISTANCE cpt 
                                JOIN HOMES h ON cpt.hhid = h.hhid
                                """)
# print(culm_car_travel)
# print(lga_person_dist)
driven_distances = duckdb.sql("""Select HOMELGA, round(median(P_DRIVEN_DISTANCE),2) AS MEDIAN_DISTANCE,
                    round(mean(P_DRIVEN_DISTANCE),2) as MEAN_DISTANCE,
                    COUNT(0) as ROW_COUNT,
                    round(stddev_samp(P_DRIVEN_DISTANCE), 2) as SAMPLE_STDDEV
                    from  lga_person_dist
                    where P_DRIVEN_DISTANCE != 0
                    group by homelga order by median(P_DRIVEN_DISTANCE)""")


cycled_distances = duckdb.sql("""Select HOMELGA, round(median(P_BICYCLED_DISTANCE),2) AS MEDIAN_DISTANCE,
                    round(mean(P_BICYCLED_DISTANCE),2) as MEAN_DISTANCE,
                    COUNT(0) as ROW_COUNT,
                    round(stddev_samp(P_BICYCLED_DISTANCE), 2) as SAMPLE_STDDEV
                    from  lga_person_dist
                    where P_BICYCLED_DISTANCE != 0
                    group by homelga order by median(P_BICYCLED_DISTANCE)""")

trained_distances = duckdb.sql("""Select HOMELGA, round(median(P_TRAINED_DISTANCE),2) AS MEDIAN_DISTANCE,
                    round(mean(P_TRAINED_DISTANCE),2) as MEAN_DISTANCE,
                    COUNT(0) as ROW_COUNT,
                    round(stddev_samp(P_TRAINED_DISTANCE), 2) as SAMPLE_STDDEV
                    from  lga_person_dist
                    where P_TRAINED_DISTANCE != 0
                    group by homelga order by median(P_TRAINED_DISTANCE)""")

# Trained distances
duckdb.sql("""select 
           CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, 
           MEDIAN_DISTANCE from trained_distances""").write_csv('Median_Train_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, MEAN_DISTANCE from trained_distances""").write_csv('Mean_Train_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, SAMPLE_STDDEV from trained_distances""").write_csv('Stddev_Train_Travel_Dist.csv')
# Cycled distances
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, MEDIAN_DISTANCE from cycled_distances""").write_csv('Median_Bicycle_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, MEAN_DISTANCE from cycled_distances""").write_csv('Mean_Bicycle_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, SAMPLE_STDDEV from cycled_distances""").write_csv('Stddev_Bicycle_Travel_Dist.csv')
# Driven distances
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, MEDIAN_DISTANCE from driven_distances""").write_csv('Median_Vehicle_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, MEAN_DISTANCE from driven_distances""").write_csv('Mean_Vehicle_Travel_Dist.csv')
duckdb.sql("""select CASE WHEN HOMELGA not in ('Melton Shire', 'Moreland City') THEN upper(HOMELGA)
           WHEN HOMELGA  = 'Melton Shire' THEN 'MELTON CITY'
           WHEN HOMELGA  = 'Moreland City' THEN 'MERRI-BEK CITY' END
           as LGA, SAMPLE_STDDEV from driven_distances""").write_csv('Stddev_Vehicle_Travel_Dist.csv')
# print(test)