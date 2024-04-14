import pandas as pd

class DataQualityManager:

    def dq_from_csv(self,CSVPATH,topic):
        # Extracting Data quality conditions from the CSV file
        df=pd.read_csv(CSVPATH)
        row=df[(df['dataset_name']==topic) & (df['dq_flag'].str.lower()=="enable")]['condition']
        good_condition=' '.join(map(str, row))
        return good_condition

    def dq_from_information_schema_uc_bronze(self,spark,topic):
        # Extracting conditions from information schema
        df = spark.sql(f"""SELECT tag_value FROM kpn_bronze.information_schema.column_tags WHERE table_name='{topic}_dt'""")
        # Collect the values into a list
        tag_values = df.select("tag_value").collect()
        # Extract the values from the Row objects and convert them to a list
        tag_values_list = ["("+row["tag_value"]+")" for row in tag_values]
        # Join the list of values into a single string
        good_condition = ' AND '.join(tag_values_list)
        return good_condition