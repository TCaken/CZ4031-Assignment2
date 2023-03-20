import psycopg2
import annotation
import re
import pandas as pd

HOST = "cz4031-db.c3eyqxhdkvkd.ap-southeast-1.rds.amazonaws.com"
PORT = 5432
DB = "postgres"
USER = "postgres"
PASSWORD = "password"

class Connection:
    """
    Using pyscopg2 to connect to the PostgreSQL DB.
    
    """    
    def __init__(self, host, port, db, user, password):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.connected = False

        try:
            self.conn = psycopg2.connect(
                host=self.host, port=self.port, database=self.db, user=self.user, password=self.password)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
            self.connected = True
        except:
            print("Connection Failed")

    def is_connected(self):
        return self.connected

    def connect(self, host, port, db, user, password):
        self.close()

        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.connected = False

        try:
            self.conn = psycopg2.connect(
                host=self.host, port=self.port, database=self.db, user=self.user, password=self.password)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
            self.connected = True
        except:
            print("Connection Failed")

    def get_cur(self):
        if self.connected:
            return self.cur

        return None

    def close(self):
        if self.connected:
            self.cur.close()
            self.conn.close()
        else:
            pass

    def __get_table_data(self, table_name):
        query = """SELECT column_name FROM information_schema.columns WHERE table_name ='""" + \
            table_name + """'"""
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        columns = []
        for x in query_results:
            columns.append(x[0])
        return columns

    def get_schema_data(self):
        if(not self.is_connected):
            return dict()

        query = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        tables = {}  # tablename: tabledata
        for x in query_results:
            tables[x[0]] = self.__get_table_data(x[0])
        return tables

    def explain_query_2(self, query):
        self.cur.execute(""" EXPLAIN (FORMAT JSON) """ + query)
        query_results = self.cur.fetchall()
        # TODO: probably have to check if have results or not from postgresql
        query_plan = query_results[0][0][0]['Plan']

        return query_plan

# ---------- METHODS FOR INTERFACE: SCHEMA INFORMATION ----------

def get_schema_data():
    """
    Retrieves all table data from current public schema.

    Returns:
        dict: tables and the columns in each table
    """    
    query = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
    cur.execute(query)
    query_results = cur.fetchall()
    tables = {}  # tablename: tabledata
    for x in query_results:
        tables[x[0]] = get_table_data(x[0])
    return tables


def get_table_data(table_name):
    """
    Retrieves all column data from table.

    Args:
        table_name (str): name of table in db

    Returns:
        list: list of column names in the table 
    """    
    query = """SELECT column_name FROM information_schema.columns WHERE table_name ='""" + \
        table_name + """'"""
    cur.execute(query)
    query_results = cur.fetchall()
    columns = []
    for x in query_results:
        columns.append(x[0])
    return columns

class SqlQuery:
    """
    A class to represent a SQL query.
    Parses an SQL query to their individual clauses.

    ...
    
    Attributes
    ----------
    select : dict
        column names in SELECT clause, aliases are the keys (if any)
    tables : dict
        non-join table names in FROM clause, aliases are the keys (if any)
    tables_join : list
        join conditions in FROM clause
    where_join : list
        join conditions in WHERE clause
    where_filter : list
        non-join conditions in WHERE clause
    group_by : list
        column names in GROUP BY clause
    order_by : list
        column names in ORDER BY clause
    limit : str
        value in LIMIT clause
    all_subqueries : list
        SqlQuery objects of subqueries nested in the current query
    all : dict 
        the above attributes in a single dictionary, for easy access

    Methods
    -------
    _standardise_sql(sql):
        Removes semicolons, returns lowercased (excluding quotes), whitespace-standardised sql.
    _extract_subqueries(sql):
        Removes all subqueries from sql, returns modified sql and list of subqueries.
    _get_next_section(string, indexes, start_value, ordered_next_values):
        Given indexes of keywords in the string, returns substring that starts with start_value, 
        and ends with the first ordered_next_value found in remaining string.    
    _extract_as(string, keyword):
        Extract column names and aliases delimited by commas.
    _extract_tables(string, keyword):
        Extract conditions of join. For non-joins, extract table names and aliases delimited by commas.
    _extract_where(string):
        Extract conditions delimited by commas, separate join from non-join conditions.
    _extract_list(string, keyword):
        Extract column names delimited by commas.
    """
    
    def __init__(self, sql):
        """
        Parses the given SQL into data structures.

        Args:
            sql (str): a SQL query
        """        
        sql = self._standardise_sql(sql)
        current_sql, subqueries = self._extract_subqueries(sql)

        # Identify keywords in the SQL query
        keywords = ["select", "from", "where", "group by", "order by", "limit"]
        indexes = {}
        for keyword in keywords:
            indexes[keyword] = current_sql.find(keyword)

        # Extract clause substrings. Identify with keyword of next clause e.g. must have FROM after SELECT
        select_section = current_sql[:indexes['from']]
        if "unique" in select_section:  # ignore unique (e.g. SELECT UNIQUE)
            select_section = select_section[select_section.index(
                "unique") + len("unique"):].strip()
        from_section = self._get_next_section(current_sql, indexes, "from", [
                                              "where", "group by", "order by", "limit"])
        where_section = self._get_next_section(
            current_sql, indexes, "where", ["group by", "order by", "limit"])
        group_by_section = self._get_next_section(
            current_sql, indexes, "group by", ["order by", "limit"])
        order_by_section = self._get_next_section(
            current_sql, indexes, "order by", ["limit"])
        limit_section = self._get_next_section(
            current_sql, indexes, "limit", [])

        # Process clause substrings to get relevant information in structured format
        self.select = self._extract_as(select_section, 'select')
        self.tables, self.tables_join = self._extract_tables(
            from_section, 'from')
        self.where_join, self.where_filter = self._extract_where(
            where_section, 'where')
        self.group_by = self._extract_list(group_by_section, 'group by')
        self.order_by = self._extract_list(order_by_section, 'order by')
        self.limit = limit_section.replace("limit", "").strip()

        self.all_subqueries = []  # array of SqlQuery objects representing subqueries
        for subquery in subqueries:
            self.all_subqueries.append(SqlQuery(subquery))

        self.all = {"select": self.select,
                    "from": self.tables,
                    "from_join": self.tables_join,
                    "where_join": self.where_join,
                    "where_filter": self.where_filter,
                    "group_by": self.group_by,
                    "order_by": self.order_by,
                    "limit": self.limit,
                    "subqueries": self.all_subqueries}

    def _standardise_sql(self, sql):
        """
        Standardises the format of the SQL query.
        Removes semicolons, standardises whitespace. Converts characters not in quotes to lowercase.

        Args:
            sql (str): a SQL query

        Returns:
            str: standardised version of the input SQL
        """        
        # exclude final semicolon, whitespace
        sql = sql.replace(";", "").replace("\n", " ").replace("\t", " ")

        # lowercase all except characters in quotes e.g. c_mktsegment = 'BUILDING'
        quotes_regex = r"(?:\"|').*?(?:\"|')"
        in_quotes = re.findall(quotes_regex, sql)
        no_quotes = re.split(quotes_regex, sql)
        len_in_quotes = len(in_quotes)
        len_no_quotes = len(no_quotes)
        tmp = ""
        for i in range(max(len_in_quotes, len_no_quotes)):
            if i < len_no_quotes:
                tmp += no_quotes[i].lower()
            if i < len_in_quotes:
                tmp += in_quotes[i]
        result = " ".join(tmp.split())  # standardise whitespace

        return result

    def _extract_subqueries(self, sql):
        """
        Extracts direct subqueries (assume delimited by brackets) from SQL query.

        Args:
            sql (str): an SQL query

        Returns:
            str: the modified SQL query, subqueries are replaced with SUBQUERY_x, where x is the subquery index
            list: list of subquery strings
        """        
        # ref: https://stackoverflow.com/questions/546433/regular-expression-to-match-balanced-parentheses
        matching_brackets_regex = r"\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)"
        strs_in_brackets = re.findall(matching_brackets_regex, sql)
        subqueries = []
        modified_sql = sql
        for x in strs_in_brackets:
            if "select" in x:  # subquery found
                modified_sql = modified_sql.replace(
                    x, "SUBQUERY_" + str(len(subqueries)), 1)
                subqueries.append(x[1:-1].strip())

        return modified_sql, subqueries

    def _get_next_section(self, string, indexes, start_value, ordered_next_values):
        """
        Given indexes of keywords in the string, returns substring that starts with start_value, 
        and ends with the first ordered_next_value found in remaining string.    

        Args:
            string (str): a string containing keywords in start_value and ordered_next_valuess
            indexes (dict): indexes of first letter of all relevant keywords in string
            start_value (str): keyword expected to start the section
            ordered_next_values (list): possible keywords to end the section

        Returns:
            str: section starting with start_value, ending just before next keyword found
        """        
        if indexes[start_value] == -1:
            return ""
        for next_value in ordered_next_values:
            if (indexes[next_value] == -1):
                continue
            return string[indexes[start_value]: indexes[next_value]]
        return string[indexes[start_value]:]

    def _extract_as(self, string, keyword):
        """
        Extract column names and aliases delimited by commas from SELECT clause.

        Args:
            string (str): a SQL query SELECT clause
            keyword (str): keyword to be removed from string

        Returns:
            dict: aliases, if any, are the keys, column names are the values
        """        
        result = {} 
        select_list = string.replace(keyword, "").strip().split(",")
        for x in select_list:
            tmp = x.strip()
            if ' as ' in tmp: # check for aliases
                val, name = tmp.split(' as ')
                result[name] = val
            else:
                result[tmp] = tmp
        return result

    def _extract_tables(self, string, keyword):
        """
        Extract conditions of join in FROM clause. 
        For non-joins, extract table names and aliases delimited by commas.

        Args:
            string (str): a SQL query FROM clause
            keyword (str): keyword to be removed from string

        Returns:
            dict: aliases, if any, are the keys, table names are the values
            list: conditions of join(s) in FROM clause
        """        
        join_condition_regex = r"(?<=\bon\b)(.*?)(?=left join|join|right join|full outer join|full join|inner join|,|$)"

        tables_result = {}
        tables_joins = []
        tables_list = string.replace(keyword, "").strip().split(",")
        for table in tables_list:
            table = table.strip()
            actual_table = ""
            table_name = ""
            if " join " in table:
                conditions = [x.strip()
                              for x in re.findall(join_condition_regex, table)]
                for i in conditions:
                    table = table.replace("on "+i, "")
                tables_joins.extend(conditions)
                continue
            elif " as " in table:
                actual_table, table_name = table.split(" as ")
            elif " " in table:
                actual_table, table_name = table.split(" ")
            else:
                actual_table = table
                table_name = table
            tables_result[table_name] = actual_table
        return tables_result, tables_joins

    def _extract_where(self, string, keyword):
        """
        Extracts conditions delimited by commas in WHERE clause.
        Separate join from non-join conditions.

        Args:
            string (str): a SQL query WHERE clause
            keyword (str): keyword to be removed from string

        Returns:
            list: join conditions (e.g. p_partkey = l_partkey)
            list: non-join conditions (e.g. p_brand = 'Brand#12' or l_commitdate < l_receiptdate)
        """        
        where_string = string.replace(keyword, "").strip()
        where_list = re.split(' and | or ', where_string)
        # check if have between. will be split into two consecutive strings around "and"
        where_join = []
        where_filter = []
        between_var = ""

        for condition in where_list:
            # if number of ( != number of ), need to remove
            bracket_diff = condition.count("(") - condition.count(")")
            if bracket_diff > 0:
                for i in range(bracket_diff):
                    index = condition.index("(")
                    condition = condition[:index] + condition[index+1:]
            elif bracket_diff < 0:
                for i in range(abs(bracket_diff)):
                    index = condition.rindex(")")
                    condition = condition[:index] + condition[index+1:]

            condition = condition.strip()

            # split 'between' statement into 2 > and < statements
            if between_var:
                condition = between_var + " < " + condition
                between_var = ""
            if " between " in condition:
                between_var, val = condition.split(" between ")
                condition = between_var + " > " + val

            # where_joins (e.g. p_partkey = l_partkey) separated from 
            # where_filters (e.g. p_brand = 'Brand#12' or l_commitdate < l_receiptdate)
            if "=" in condition and not re.search("['\"]|(=(\s)*\d)", condition):
                where_join.append(condition)
            else:
                where_filter.append(condition)

        return where_join, where_filter

    def _extract_list(self, string, keyword):
        """
        Extract column names delimited by commas.

        Args:
            string (str): a SQL query GROUP BY or ORDER BY clause
            keyword (str): keyword to be removed from string 

        Returns:
            list: list of column names
        """        
        return [x.strip() for x in string.replace(keyword, "").strip().split(",")]

    def __str__(self):
        """
        String representation of the parsed SQL query.

        Returns:
            str: the SQL query as a string
        """        
        output = "\n------------------------------------------------------------------"
        output += "\nSELECT:\t" + str(self.select) \
            + "\nFROM:\t" + str(self.tables) \
            + "\nFROM_JOIN:\t" + str(self.tables_join) \
            + "\nWHERE_JOIN:\t" + str(self.where_join) \
            + "\nWHERE_FILTER:\t" + str(self.where_filter) \
            + "\nGROUP BY:\t" + str(self.group_by) \
            + "\nORDER BY:\t" + str(self.order_by) \
            + "\nLIMIT:\t" + str(self.limit)
        for i in range(len(self.all_subqueries)):
            output += "\n------SUBQUERY " + \
                str(i) + "------" + self.all_subqueries[i].__str__()
        output += "\n------------------------------------------------------------------"
        return output


class Node:
    def __init__(self, node_dict, node_index, parsed_query):
        self.left = None
        self.right = None
        self.node_index = node_index
        tmp = node_dict.copy()
        tmp.pop('Plans', None)
        self.data = tmp
        self.annotation = {}
        self.alt_node = []
        self.alt_cost = []

    def __str__(self):
        return "" + str(self.node_index) + "   " + str(self.data)

class Tree:
    """
    Represents a query plan generated by PostgreSQL, and is made of nodes.

    """    
    def __init__(self, postgres_queryplan, parsed_sql, query_string):
        self.current_index = 0
        self.root = None
        self.edgeList = []
        self.nodesList = []
        self.fullNodesList = []
        self.parsed_query = parsed_sql
        self.all_annotations = []
        self.query_string = query_string
        self.root = self.build_tree(postgres_queryplan)
        self.operations = []
        self.get_operations_list()
        print(self.operations)
        # self.get_multiple_aqp(self.file_path, self.operations)

    # build tree based on the output of query EXPLAIN and create a full node list mapping joins and scans

    def build_tree(self, node):
        """
        Builds the tree based on the parsed query attribute in the tree.

        Args:
            node (str): node representation in query plan

        Returns:
            Node: a created node (with instantiated children nodes)
        """        
        cur = Node(node, self.current_index, self.parsed_query)
        # self.all_annotations.append(cur.annotation)
        self.current_index += 1
        self.fullNodesList.append(cur)
        if self.root == None:
            self.root = cur
        if "Plans" not in node:
            # leaf node
            return cur

        cur.left = self.build_tree(node["Plans"][0])
        if len(node["Plans"]) > 1:
            cur.right = self.build_tree(node["Plans"][1])

        return cur

    # build the edgelist and nodelist to pass to networkx for visualisation
    def get_networkx_info(self, cur, num_indent=0):
        """
        Populates the nodesList and the edgeList to which are passed to the networkx to generate the graph.

        Args:
            cur (Node): root node of the tree
        """        
        self.nodesList.append(
            (cur.node_index, cur.data['Total Cost'], cur.data['Node Type'], cur.alt_node, cur.alt_cost))
        if cur.left != None:
            self.edgeList.append((cur.node_index, cur.left.node_index))
            self.get_networkx_info(cur.left, num_indent+1)
        if cur.right != None:
            self.edgeList.append((cur.node_index, cur.right.node_index))
            self.get_networkx_info(cur.right, num_indent+1)

    # displays tree in CLI
    def display_tree(self, cur, num_indent=0):
        """
        Displays the built tree.

        Args:
            cur (Node): current node
            num_indent (int, optional): Level of the tree. Defaults to 0.
        """        
        print("LEVEL", num_indent, ": ", cur)
        if cur.left != None:
            self.display_tree(cur.left, num_indent+1)
        if cur.right != None:
            self.display_tree(cur.right, num_indent+1)

    def get_operations_list(self):
        """
        Creates a list of scans and joins which are stored as an attribute operations.
        """        
        for node in self.fullNodesList:
            if node.data['Node Type'] in self.operations:
                continue
            if 'Join Type' in node.data:
                self.operations.append(node.data['Node Type'])
            elif 'Scan' in node.data['Node Type']:
                #if node.data['Node Type'] != 'Seq Scan':
                self.operations.append(node.data['Node Type'])
        self.operations = [*set(self.operations)]
        # self.operations_dict = dict.fromkeys(operators, 0)

    def get_multiple_aqp(self, query_string, operations, cur):
        """
        Generates multiple query plans by turning off different scans or joins and stores them in a list.

        Args:
            query_string (str): raw query
            operations (list): operations list in the tree
            cur (Connection): connection cursor

        Returns:
            list: list of AQP query plans
        """        
        aqp_query_results = []
        print("--------------------------------------------------------------------------------------------------------")
        for i in range(len(operations)):
            operator = operations[i]
            if operator == 'Nested Loop':
                cur.execute(""" SET ENABLE_NESTLOOP TO OFF """)
            elif operator == 'Hash Join':
                cur.execute(""" SET ENABLE_HASHJOIN TO OFF """)
            elif operator == "Merge Join":
                cur.execute(""" SET ENABLE_MERGEJOIN TO OFF """)
            elif operator == "Index Only Scan":
                cur.execute(""" SET ENABLE_INDEXONLYSCAN TO OFF """)
            elif operator == 'Index Scan':
                cur.execute(""" SET ENABLE_INDEXSCAN TO OFF """)
            elif operator == 'Bitmap Scan':
                cur.execute(""" SET ENABLE_BITMAPSCAN TO OFF """)
            # cur.execute(""" SET ENABLE_BITMAPSCAN TO OFF """)
            cur.execute(""" EXPLAIN (FORMAT JSON) """ + query_string)
            print(query_string)
            query_results = cur.fetchall()
            query_plan = query_results[0][0][0]['Plan']
            aqp_query_results.append(query_plan)
        cur.execute(""" SET ENABLE_NESTLOOP TO ON """)
        cur.execute(""" SET ENABLE_HASHJOIN TO ON """)
        cur.execute(""" SET ENABLE_MERGEJOIN TO ON """)
        cur.execute(""" SET ENABLE_INDEXONLYSCAN TO ON """)
        cur.execute(""" SET ENABLE_INDEXSCAN TO ON """)
        cur.execute(""" SET ENABLE_BITMAPSCAN TO ON """)

        return aqp_query_results

    def generate_altnodes(self, tree, cur):
        """
        Generate the AQPs based on the current QEP. 
        
        Args:
            tree (Tree): QEP tree 
            cur (Connection): connection cursor 
        """        
        aqp_query_results = self.get_multiple_aqp(
            self.query_string, self.operations, cur)
        aqp_trees_list = []
        for q in aqp_query_results:
            aqp_tree = Tree(q, self.parsed_query, self.query_string)
            aqp_tree.display_tree(aqp_tree.root)
            aqp_trees_list.append(aqp_tree)
        for aqp in aqp_trees_list:
            node_mapper(tree, aqp)


# use to map joins 1-1 , WILL FAIL if both have different amount of joins
def join_mapper(list1, list2):
    """
    Iterates through both lists to perform 1-1 mapping of the join nodes.

    Args:
        list1 (list): fullNodesList from main QEP tree
        list2 (list): fullNodesList from AQP tree

    Returns:
        dataframe: mapped dataframe
    """    
    df = pd.DataFrame(columns=['QP index', 'QP', 'QP cost'])
    qdf = pd.DataFrame(columns=['AQP index', 'AQP', 'AQP cost'])
    l1 = 0
    l2 = 0
    while l1 < len(list1):
        # check if node data from the main QP has attribute 'Join Type'
        if 'Join Type' in list1[l1].data:
            new_row = {'QP index': list1[l1].node_index,
                       'QP': list1[l1].data['Node Type'],
                       'QP cost': list1[l1].data['Total Cost']}
            df = pd.concat([df, pd.DataFrame([new_row])])
            # check if node data from the AQP has attribute 'Join Type'
            if(l2 < len(list2)):
                while('Join Type' not in list2[l2].data):
                    l2 += 1
                else:
                    new_row = {'AQP index': list2[l2].node_index,
                               'AQP': list2[l2].data['Node Type'],
                               'AQP cost': list2[l2].data['Total Cost']}
                    # print(new_row)
                    qdf = pd.concat([qdf, pd.DataFrame([new_row])])
                    l2 += 1
        l1 += 1

    df = pd.concat([df, qdf], axis=1)

    df = df.drop(df[df['QP'] == df['AQP']].index)
    print("JOIN MAP")
    print(df)
    return df


# use to map scans 1-1, WILL FAIL if both have different amount of scans
def scan_mapper(list1, list2):
    """
    Iterates through both list to perform 1-1 mapping of scan nodes.

    Args:
        list1 (list): fullNodesList from the main QEP tree
        list2 (list): fullNodesList from the AQP tree

    Returns:
        dataframe: mapped dataframe
    """    
    df = pd.DataFrame(columns=['QP index', 'QP', 'QP cost', 'QP source'])
    qdf = pd.DataFrame(columns=['AQP index', 'AQP', 'AQP cost', 'AQP source'])
    l1 = 0
    l2 = 0
    while l1 < len(list1):
        # Check if Node Type attribute in node data from the QP is a scan
        if 'Scan' in list1[l1].data['Node Type']:
            if 'Bitmap Index Scan' in list1[l1].data['Node Type']:
                relation_name, index = list1[l1].data['Index Name'].split('_')
            else:
                relation_name = list1[l1].data['Relation Name']

            new_row = {'QP index': list1[l1].node_index,
                       'QP': list1[l1].data['Node Type'],
                       'QP cost': list1[l1].data['Total Cost'],
                       'QP source': relation_name}
            df = pd.concat([df, pd.DataFrame([new_row])])

            # Check if Node Type attribute in node data from the AQP is a scan
            while('Scan' not in list2[l2].data['Node Type']):
                l2 += 1
            else:
                print(list2[l2].data['Node Type'])
                if 'Bitmap Index Scan' in list2[l2].data['Node Type']:
                    relation_name, index = list2[l2].data['Index Name'].split(
                        '_')
                else:
                    relation_name = list2[l2].data['Relation Name']
                new_row = {'AQP index': list2[l2].node_index,
                           'AQP': list2[l2].data['Node Type'],
                           'AQP cost': list2[l2].data['Total Cost'],
                           'AQP source': relation_name}
                qdf = pd.concat([qdf, pd.DataFrame([new_row])])
                l2 += 1
        l1 += 1
    df = df.sort_values(by=['QP source'])
    qdf = qdf.sort_values(by=['AQP source'])
    df = pd.concat([df, qdf], axis=1)
    # df = pd.merge(df, qdf, on='source', how='outer')
    # df = pd.DataFrame(df[df.index_x == df.index_y]['source'],
    #                   columns=['source']).reset_index(drop=True)
    # print(df)
    print("SCAN MAP BEFORE FILTER")
    print(df)
    df = df.drop(df[df['QP cost'] > df['AQP cost']].index)
    df = df.loc[df['AQP'] != df['QP']]
    print("SCAN MAP")
    print(df)
    return df



# combine both join mapper and scan mapper to map joins and scans, takes in the QP node index to be mapped as input and outputs the AQP node index
def node_mapper(tree, aqp_tree):
    """
    Combines the outputs of the join_mapper and scan_mapper into a single dataframe. 
    It will then call add_alt_attributes function to individually add the alternate nodes and 
    costs to the alt_node and alt_cost list of each node.

    Args:
        tree (Tree): the QEP tree
        aqp_tree (Tree): the AQP tree
    """    
    join_df = join_mapper(tree.fullNodesList, aqp_tree.fullNodesList)
    scan_df = scan_mapper(tree.fullNodesList, aqp_tree.fullNodesList)
    combined_df = pd.concat([join_df, scan_df], axis=0)
    combined_df = combined_df.sort_values(by=['QP index'])
    combined_df = combined_df.reset_index(drop=True)
    # return combined_df
    print("COMBINED MAP")
    print(combined_df)
    print("------------------------------------------------------------------")
    add_alt_attributes(tree.root, combined_df, tree)
    print(tree.all_annotations)



# add_alt_attributes to the nodes in the main tree
def add_alt_attributes(cur, combined_df, tree):
    """
    Recursively travels the tree and append alternate nodes to the alt_node list for each node.
    Appends the alternate cost to the alt_cost for each node in the main QEP tree.

    Args:
        cur (Node): node of the tree
        combined_df (dataframe): dataframe with mapped nodes
        tree (Tree): the QEP tree 
    """
    try:
        target_row = combined_df.loc[combined_df['QP index'] == cur.node_index]
        alt_node = target_row['AQP'].item()
        alt_cost = target_row['AQP cost'].item()
        if alt_node not in cur.alt_node:
            cur.alt_node.append(alt_node)
            cur.alt_cost.append(alt_cost)
        print(cur.alt_node, cur.alt_cost)

        print(cur.annotation)
        # print(cur.alt_node, cur.alt_cost)
    except:
        #print("No substitute node")
        pass
    cur.annotation = annotation.MapAnnotation(tree.parsed_query.all, cur.data, cur.alt_node, cur.alt_cost)

    tree.all_annotations.append(cur.annotation)
    if cur.left != None:
        add_alt_attributes(cur.left, combined_df, tree)
    if cur.right != None:
        add_alt_attributes(cur.right, combined_df, tree)


# ---------- CODE FOR TESTING ----------
aws = Connection(HOST, PORT, DB, USER, PASSWORD)
cur = aws.get_cur()

# use this to check if you are using the correct port
cur.execute("""SELECT version()""")
print("connected to ", cur.fetchone())

# # check if can access tables
# cur.execute(
#     """SELECT table_name FROM information_schema.tables WHERE table_schema='public'""")
# query_results = cur.fetchall()
# print(query_results)

# Load TPC-H data to the db if empty
# Place TPC-H csvs in 'C:\Users\Public\csvs'
# fd = open('initialise_db.sql', 'r')
# queries = fd.read()
# fd.close()
# for query in queries.split(";\n\n"):
#     try:
#         cur.execute(query)
#     except:
#         print("Table already loaded")

# get single query from single file
def get_query(file_path):
    """
    Read SQL query from file.

    Args:
        file_path (str): path of file containing SQL query

    Returns:
        str: the SQL query
    """    
    fd = open(file_path, 'r')
    query = fd.read()
    fd.close()
    return query

def explain_query(query):
    """
    Get query plan from PostgreSQL.

    Args:
        query (str): the SQL query

    Returns:
        str: the query plan, in JSON format
    """
    cur.execute(""" EXPLAIN (FORMAT JSON) """ + query)
    query_results = cur.fetchall()
    query_plan = query_results[0][0][0]['Plan']
    return query_plan

query_path = "./queries/6.sql"
# query_plan = explain_query()
parsed_sql = SqlQuery(get_query(query_path))  # parses sql to good format

# # get main query plan
query_string = """
SELECT
    sum(l_extendedprice* (1 - l_discount)) as revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 1 + 10
        AND p_size between 1 AND 5
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 10 + 10
        AND p_size between 1 AND 10
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 20 + 10
        AND p_size between 1 AND 15
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    );
"""

# query_plan = explain_query(query_string)
# parsed_sql = SqlQuery(query_string)

# tree = Tree(query_plan, parsed_sql, query_string)

# # print("ALL ANNOTATIONS:", tree.all_annotations)
# print("TREE STRUCTURE: ")
# tree.display_tree(tree.root)

# print("ALTERNATE TREES")
# tree.generate_altnodes(tree, cur)

# annotation.generate_diagram(tree.nodesList, tree.edgeList)
# annotation.plt.show()

# # get AQP
# aqp = get_aqp(query_path, """ ENABLE_HASHJOIN """)
# aqp_tree = Tree(aqp, parsed_sql)
# aqp_tree.display_tree(aqp_tree.root)
# aqp_tree.get_networkx_info(aqp_tree.root)
# # annotation.generate_diagram(aqp_tree.nodesList, aqp_tree.edgeList)
# # annotation.plt.show()

# # can only call get_networkx_info after calling the aqp and node mapper so that the nodes will be updated with the alt nodes
# node_mapper(tree, aqp_tree)
# tree.get_networkx_info(tree.root)
# annotation.generate_diagram(tree.nodesList, tree.edgeList)
# annotation.plt.show()

# aqp_query_results = get_multiple_aqp(query_path, tree.operations)
# aqp_trees_list = []
# for q in aqp_query_results:
#     aqp_tree = Tree(q, parsed_sql)
#     aqp_tree.display_tree(aqp_tree.root)
#     aqp_trees_list.append(aqp_tree)

# # print(aqp_trees_list)
# for aqp in aqp_trees_list:
#     node_mapper(tree, aqp)

# print(parsed_sql.all)
# print(postgres_tree.root)
# print("this is the tree")
# print(annotation.MapAnnotation(parsed_sql.all,postgres_tree.root))

aws.close()
