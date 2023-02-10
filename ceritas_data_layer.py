import psycopg2

class Ceritas_Database:
    def __init__(self, database, user, password, host):
        self._conn = psycopg2.connect(database=database,
                user=user,
                password=password,
                host=host)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    # helper function, to convert list of columns to sql
    def list_to_sql(self, this_list):
        if type(this_list) is not list: this_list = [ this_list ]
        sql = ""
        for item in this_list:
            sql = sql + item + ', '
        sql = sql[:-2] # remove final comma
        return sql

    # retrieves all rows from a table. 
    # table: string. name of table to pull from
    # column: optional, string or list of strings. column(s) to pull instead of all columns
    def get_all_from_table(self, table, column=None):
        if column is not None:
            column = self.list_to_sql(column)
        sql = "SELECT {columns} FROM {table};".format(columns=column or '*', table=table)
        self.cursor.execute(sql)
        result = self.fetchall()
        items = []
        for item in result:
            items.append(item)
        return items

    # returns the number of rows from a table. 
    # "nonnull" is the name of a column, if you wish to only count rows where this column is not null
    def get_count_from_table(self, table, nonnull=None):
        sql = "SELECT COUNT(*) FROM {table}".format(table=table)
        if nonnull is not None:
            sql = sql + " WHERE {nonnull} IS NOT NULL;".format(nonnull=nonnull)
        else:
            sql = sql + ";"
        self.cursor.execute(sql)
        result = self.fetchall()
        return result[0][0]

    # returns all product_id's associated with customers. can fill in column parameter to get other or all columns
    def get_all_customer_products(self, column=None):
        if column is not None:
            column = self.list_to_sql(column)
        self.cursor.execute("SELECT id FROM groups WHERE is_default = true;")
        default_groups = self.fetchall()

        product_instance_ids = tuple()

        for group in default_groups:
            self.cursor.execute("SELECT product_instance_id FROM product_instance_group WHERE group_id = %s;", (group[0],))
            result = self.fetchall()
            for item in result:
                product_instance_ids = product_instance_ids + item

        self.cursor.execute("SELECT product_id FROM product_instances WHERE id in %s;", (product_instance_ids,))
        result = self.fetchall()
        items = []
        for item in result:
            items.append(item[0])
        return items

    # returns all product_ids for a specific customer. can select specific columns besides product_id to fetch
    def get_one_customer_products(self, customer, column=None):
        if column is not None:
            column = self.list_to_sql(column)

        self.cursor.execute("SELECT id FROM groups WHERE is_default = true AND name = %s;", (customer,))
        default_groups = self.fetchall()

        product_instance_ids = tuple()

        for group in default_groups:
            self.cursor.execute("SELECT product_instance_id FROM product_instance_group WHERE group_id = %s;", (group[0],))
            result = self.fetchall()
            for item in result:
                product_instance_ids = product_instance_ids + item

        self.cursor.execute("SELECT product_id FROM product_instances WHERE id in %s;", (product_instance_ids,))
        result = self.fetchall()
        items = []
        for item in result:
            items.append(item[0])
        return items

    # core_product_id: id or list of id's to pull product info from
    # column: string or list of strings, specifying which columns to pull down. if blank, all columns are pulled down.
    def get_product_info_by_id(self, core_product_id, column=None):
        if column is not None:
            column = self.list_to_sql(column)
        if type(core_product_id) is not list: core_product_id = [ core_product_id ]
        product_tuple = tuple(core_product_id)

        self.cursor.execute("SELECT {column} FROM core_products WHERE id IN %s;".format(column=column or "*"), (product_tuple,))
        return self.fetchall()

    # no inputs. grabs all core_product_ids which are linked to nvd_products
    def get_vulnerable_products(self):
        self.cursor.execute("SELECT core_product_id FROM nvd_products WHERE core_product_id IS NOT NULL;")
        result = self.fetchall()
        product_id_list = []
        for item in result:
            product_id_list.append(item[0])
        return product_id_list

    # product_id: product id or list of ids.
    # column: string or list of strings, column names to pull from core_rating_history table. "value" is pulled by default
    # returns rating value for each id passed. 
    def get_product_rating(self, product_id, column=None):
        if type(product_id) is not list: product_id = [ product_id ]
        product_id_tuple = tuple()
        for id in product_id:
            product_id_tuple = product_id_tuple + (id,)

        self.cursor.execute("SELECT current_rating_history_id FROM core_products WHERE id IN %s;", (product_id_tuple,))
        result = self.fetchall()

        core_rating_history_ids = tuple()
        for item in result:
            core_rating_history_ids = core_rating_history_ids + item

        self.cursor.execute("SELECT {column} FROM core_rating_history WHERE id IN %s;".format(column=column or "value"), (core_rating_history_ids,))
        result = self.fetchall()

        rating_value_list = []
        for item in result:
            rating_value_list.append(item[0])

        return rating_value_list

    # pass in product_id: get all vulnerabilities connected to this product
    # pass in list of product_id: get all vulnerabilities for list of products
    # pass in nothing for product_id: vulnerabilities for all core products
    def get_product_vulnerability_ids(self, product_id=None):
        if product_id is not None:
            if type(product_id) is not list: product_id = [ product_id ]
            product_id_tuple = tuple()
            for id in product_id:
                product_id_tuple = product_id_tuple + (id,)
            
            self.cursor.execute("SELECT id FROM nvd_products WHERE core_product_id IN %s;", (product_id_tuple,))
        else:
            self.cursor.execute("SELECT id FROM nvd_products WHERE core_product_id IS NOT NULL;")

        result = self.fetchall()
        nvd_product_ids = tuple()
        for item in result:
            nvd_product_ids = nvd_product_ids + item
        
        if len(nvd_product_ids) > 0:
            self.cursor.execute("SELECT id FROM nvd_cpe_matches WHERE nvd_product_id in %s", (nvd_product_ids,))
            result = self.fetchall()
            
            nvd_cpe_match_ids = tuple()
            for item in result:
                nvd_cpe_match_ids = nvd_cpe_match_ids + item

            self.cursor.execute("SELECT nvd_configuration_id FROM nvd_cpe_match_configuration WHERE nvd_cpe_match_id IN %s", (nvd_cpe_match_ids,))
            result = self.fetchall()
            
            nvd_configuration_ids = tuple()
            for item in result:
                nvd_configuration_ids = nvd_configuration_ids + item

            self.cursor.execute("SELECT nvd_cve_id FROM nvd_configuration_cve WHERE nvd_configuration_id IN %s;", (nvd_configuration_ids,))
            result = self.fetchall()
            
            nvd_cve_id_list = []
            for item in result:
                nvd_cve_id_list.append(item[0])

            return nvd_cve_id_list
        else:
            return -1

    # nvd_cve_id: an nvd_cve_id or list of ids.
    # column: column name or list of names. optional. specifies specific columns to pull rather than all
    def get_cve_info_by_id(self, nvd_cve_id, column=None):
        if column is not None:
            column = self.list_to_sql(column)
        if type(nvd_cve_id) is not list: nvd_cve_id = [ nvd_cve_id ]
        nvd_cve_tuple = tuple(nvd_cve_id)

        if len(nvd_cve_tuple) > 0:
            self.cursor.execute("SELECT {column} FROM nvd_cves WHERE id IN %s;".format(column=column or "*"), (nvd_cve_tuple,))
            return self.fetchall()
        else:
            return -1

    # cve: cve string, for example: "CVE-2022-2222"
    # column: column name or list of names. optional. pull specific columns, or all columns if blank
    def get_cve_info_by_name(self, cve, column=None):
        if column is not None:
            column = self.list_to_sql(column)
        if type(cve) is not list: cve = [ cve ]
        cve = tuple(cve)

        self.cursor.execute("SELECT {column} FROM nvd_cves WHERE cve IN %s;".format(column=column or "*"), (cve,))
        return self.fetchall()

    def get_all_severities_for_product(self, product_id):
        cve_ids = self.get_product_vulnerability_ids(product_id)
        severities = self.get_cve_info_by_id(cve_ids, 'severity')
        return severities