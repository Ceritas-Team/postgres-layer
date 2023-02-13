import ceritas_data_layer
import pandas as pd

# Will need to enter credentials here
db_instance = ceritas_data_layer.Ceritas_Database(database, user, password, host)

# here, we are grabbing all products from a customer called "Veritech". we pass in "*" for columns to get all values.
products = db_instance.get_one_customer_products('Veritech', "*")

# prodcuts are returned as a list of dictionaries, that can be fed right into a dataframe
veritech_product_df = pd.DataFrame(products)
veritech_product_df.to_csv("test_files/get_one_customer_products.csv")

# here, we get all rows from table "nvd_products", but only the columns "cpe_short_product" and "id"
nvd_products = db_instance.get_all_from_table("nvd_products", ["cpe_short_product", "id"])

nvd_product_df = pd.DataFrame(nvd_products)
nvd_product_df.to_csv("test_files/get_all_from_table.csv")

# here we are getting all "vulnerable products", meaning, products linked to nvd_products. all columns are pulled
vulnerable_products = db_instance.get_vulnerable_products("*")
vulnerable_product_df = pd.DataFrame(vulnerable_products)

# "get_product_rating": values are returned as a list, instead of dictionaries. here we are adding a "rating" column to this dataframe
# where no rating exists, the list will contain "None" in that spot
vulnerable_product_df["rating"] = db_instance.get_product_rating(vulnerable_product_df.id.values.tolist())

vulnerable_product_df.to_csv("test_files/get_vulnerable_and_get_ratings.csv")

# for each vulnerable product we are first getting vulnerabilty ids, which comes back as a list
vulnerability_ids = db_instance.get_product_vulnerability_ids(vulnerable_product_df.id.values.tolist())
# next we are getting cve info, meaning all columns, for all these vulnerabilities. this is a dict that is read into a dataframe
cves = db_instance.get_cve_info_by_id(vulnerability_ids)

cve_df = pd.DataFrame(cves)
cve_df.to_csv("test_files/get_vulnerability_ids_and_get_cve_info.csv")

# this next part is testing "get_cve_info_by_name", which uses cve strings to query the cve table
cve_strings = cve_df.cve.values.tolist()
cve_df2 = pd.DataFrame(db_instance.get_cve_info_by_name(cve_strings, ['cve', 'severity', 'date_modified']))
cve_df2.to_csv("test_files/get_cve_info_by_name.csv")

# lastly, we are getting all severities for vulnerable products. severities come back as a list of numbers for each product
vulnerable_product_df['severites'] = vulnerable_product_df['id'].apply(lambda id : db_instance.get_all_severities_for_product(id))
vulnerable_product_df.to_csv("test_files/get_all_severites.csv")