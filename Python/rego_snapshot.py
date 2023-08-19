import duckdb
import os

def write_csv(self, name):
    self.to_csv(os.path.join(os.getcwd(),'output_data',name))
duckdb.DuckDBPyRelation.write_csv = write_csv



REGISTRATIONS = duckdb.read_csv(os.path.join(os.getcwd(),
                                    'external_data', 
                                    'Whole_Fleet_Vehicle_Registration_Snapshot_by_Model_Q2_2023.csv'),
                                    na_values='N/A',
                                    dtype = {
                                        "D_MAKE_VEH1" : "VARCHAR",
                                        "CD_CLASS_VEH": "INTEGER",
                                        "NB_YEAR_MFC_VEH" : "INTEGER",
                                        "POSTCODE" : "VARCHAR",
                                        "CD_CL_FUEL_ENG" : "VARCHAR",
                                        "TOTAL1": "INTEGER"
                                    })
print(duckdb.sql("select distinct cd_cl_fuel_eng from REGISTRATIONS"))
REGISTRATIONS_CLEANED = duckdb.sql("""
                            Select 
                            D_MAKE_VEH1,
                            CD_CLASS_VEH,
                            NB_YEAR_MFC_VEH,
                            POSTCODE,
                            trim(CD_CL_FUEL_ENG) as CD_CL_FUEL_ENG,
                            TOTAL1
                            from REGISTRATIONS where 
                            cast(POSTCODE as Integer) >= 3000 and 
                            cast(POSTCODE as Integer) <= 3999 
                            and trim(CD_CL_FUEL_ENG) in ('D','P','M','G', 'E')""")

print(REGISTRATIONS_CLEANED)

CULM_REGISTRATIONS = duckdb.sql("""select POSTCODE, 
                                sum(case when CD_CL_FUEL_ENG = 'E' then 1 else 0 end) as EV_REG_COUNT,
                                sum(case when CD_CL_FUEL_ENG = 'E' then 0 else 1 end) as NON_EV_REG_COUNT from 
                                REGISTRATIONS_CLEANED group by POSTCODE order by sum(case when CD_CL_FUEL_ENG = 'E' then 1 else 0 end) desc""")
print(CULM_REGISTRATIONS)
#################################################################################################
#################################################################################################
#############################    Read in Postcode to LGA mapping    #############################
#################################################################################################
#################################################################################################



D_LGA_POSTCODES = duckdb.read_csv(os.path.join(os.getcwd(),
                                    'mappings', 
                                    'postcode_mappings.csv'),
                                    dtype = {
                                        "OFFICIALNM" : "VARCHAR",
                                        "LGA_CODE": "INTEGER",
                                        "intersection_pct" : "DOUBLE",
                                        "POSTCODE" : "VARCHAR",
                                    })
LGA_POSTCODES = duckdb.sql(""" Select 
                            LTRIM(RTRIM(OFFICIALNM)) as OFFICIALNM,
                            LGA_CODE,
                            intersection_pct,
                            LTRIM(RTRIM(POSTCODE)) as POSTCODE
                            FROM D_LGA_POSTCODES""")
print(LGA_POSTCODES)


EVS_PER_LGA = duckdb.sql("""select 
                            lp.OFFICIALNM,
                            lp.intersection_pct,
                            lp.POSTCODE,
                            cr.EV_REG_COUNT,
                            cr.NON_EV_REG_COUNT
                            from LGA_POSTCODES lp 
                            JOIN CULM_REGISTRATIONS cr
                            on cr.POSTCODE = lp.POSTCODE
                            """)
print(EVS_PER_LGA)


CULM_EVS_PER_LGA = duckdb.sql("""SELECT OFFICIALNM, round(sum(intersection_pct*EV_REG_COUNT),0) as EVs,
                                round(sum(intersection_pct*NON_EV_REG_COUNT),0) as nonEVs,
                                round(sum(intersection_pct*EV_REG_COUNT)/(sum(intersection_pct*NON_EV_REG_COUNT)+sum(intersection_pct*EV_REG_COUNT)),4) as EV_PROPORTION
                                FROM EVS_PER_LGA GROUP BY OFFICIALNM 
                                order by 
                                round(sum(intersection_pct*EV_REG_COUNT)/(sum(intersection_pct*NON_EV_REG_COUNT)+sum(intersection_pct*EV_REG_COUNT)),4)""")

#LGA_EVS
duckdb.sql("SELECT OFFICIALNM as LGA, EVs as EV_COUNT from CULM_EVS_PER_LGA").write_csv("LGA_EVs.csv")
# LGA_NONEVS
duckdb.sql("SELECT OFFICIALNM as LGA, nonEVs as NON_EV_COUNT from CULM_EVS_PER_LGA").write_csv("LGA_nonEVs.csv")
# LGA_EV_PROPORTION
duckdb.sql("SELECT OFFICIALNM as LGA, EV_PROPORTION from CULM_EVS_PER_LGA").write_csv("LGA_EV_PROPORTION.csv")


