"""
Annotation functions for each node type
"""

#Aggregate
def aggregate_annotate(plan):
    """
    This function generates an annotation for the Aggregate operation. 
    Different annotations are generated based on the 'Strategy','Sorted','Group Key','Filter', 'Hashed', and 'Plain' 
    node attributes in the query plan
    
    :param plan:
    :return Annotation for aggregate node type:
    """
    strategy = plan["Strategy"]


    if strategy == "Sorted":
        annotation = f"The {plan['Node Type']} operation sorts the tuples based on their keys, "


        if "Group Key" in plan:
            annotation += f" where the tuples are {'aggregated'} by the following keys: "

            for key in plan["Group Key"]:
                annotation += key + ","

            annotation = annotation[:-1]
            annotation += "."


        if "Filter" in plan:
            annotation += f" where the tuples are filtered by {plan['Filter'].replace('::text', '')}."

        return annotation


    elif strategy == "Hashed":
        annotation = f"The {plan['Node Type']} operation {'hashes'} all rows based on these key(s): "

        # Obtain the attributes that the records are grouped by
        for key in plan["Group Key"]:
            # Remove unnecessary strings
            annotation += key.replace("::text", "") + ", "

        annotation += f"which are then {'aggregated'} into a bucket given by the hashed key."

        return annotation

    # Plain strategy
    elif strategy == "Plain":
        return f"The result is {'aggregated'} with the {plan['Node Type']} operation."


    else:
        raise ValueError("Annotation not supported: " + strategy)
#Append
def append_annotate(plan):
    """
    This function generates an annotation for the Append operation. 

    :param plan:
    :return Annotation for Append node type:
    """
    return f"The {plan['Node Type']} operation generates the concatenation of the results of sub-plans."

#CTE Scan
def cte_annotate(plan):
    """
    This function generates an annotation for the CTE Scan operation. 
    Different annotations are generated based on the 'CTE Name','Index Cord' and 'Filter' node attributes in the
     query plan

    :param plan:
    :return Annotation for CTE Scan node type:
    """
    annotation = f"The {plan['Node Type']} operation is performed  a common table expression on the table {str(plan['CTE Name'])} with the following conditions:"

    if "Index Cond" in plan:
        annotation += " " + plan["Index Cond"].replace("::text", "")


    if "Filter" in plan:
        annotation += " It is then filtered by " + plan["Filter"].replace("::text", "")


    annotation += ". The results are stored temporarily to be used later."
    return annotation

#Function Scan
def function_scan_annotate(plan):
    """
    This function generates an annotation for the Function Scan operation. 

    :param plan:
    :return Annotation for Function Scan node type:
    """
    return f"The {plan['Node Type']} operation scans the result of a set-returning function and returns them as record sets."

#Group
def group_annotate(plan):
    """
    This function generates an annotation for the Group operation. 
    Different annotations are generated based on the 'Group Key' node attribute in the query plan

    :param plan:
    :return Annotation for Group node type:
    """
    annotation = f"The {plan['Node Type']} operation will group the results by the following keys: "

    for i, key in enumerate(plan["Group Key"]):

        annotation += key.replace("::text", "")

        if i == len(plan["Group Key"]) - 1:
            annotation += "."
        else:
            annotation += ", "

    return annotation

#Gather Merge
def gather_merge_annotate(plan):
    """
    This function generates an annotation for the Gather Merge operation. 

    :param plan:
    :return Annotation for Gather Merge node type:
    """
    return f"The {plan['Node Type']} operation merges the results of pre-sorted worker outputs."

#Index Scan
def index_scan_annotate(plan):
    """
    This function generates an annotation for the Index Scan operation. 
    Different annotations are generated based on the 'Node Type', 'Index Cord' and 'Filter' node attributes in the 
    query plan

    :param plan:
    :return Annotation for Index Scan node type:
    """
    annotation = f"The {plan['Node Type']} operation uses an index and scans for tuples"

    if "Index Cond" in plan:
        annotation += " with the following conditions: " + plan["Index Cond"].replace("::text", "")


    # annotation += f", and then reads the records from the table that match the conditions."

    if "Filter" in plan:
        annotation += f" The results are then filtered by {plan['Filter'].replace('::text', '')}."

    return annotation


#Index Only Scan
def index_only_scan_annotate(plan):
    """
    This function generates an annotation for the Index Only Scan operation. 
    Different annotations are generated based on the 'Node Type', 'Index Cord' and 'Filter' node attributes in the 
    query plan

    :param plan:
    :return Annotation for Index Only Scan node type:
    """
    annotation = f"The {plan['Node Type']} operation scans the {plan['Index Name']} table for tuples"

    if "Index Cond" in plan:
        annotation += " with the following conditions: " + plan["Index Cond"].replace("::text", "")


    annotation += "and returns tuples that matches the conditions."

    if "Filter" in plan:
        annotation += f" The results are filtered by {plan['Filter'].replace('::text', '')}."

    return annotation

#Limit
def limit_annotate(plan):
    """
    This function generates an annotation for the Limit operation. 

    :param plan:
    :return Annotation for Limit node type:
    """
    return f"The {plan['Node Type']} operation takes data from a child node and produces a sorted output, using either memory if available or “spilling” to disk memory."

#Materialize
def materialize_annotate(plan):
    """
    This function generates an annotation for the Materialize operation. 
    :param plan:
    :return Annotation for Materialize node type:
    """
    return f"The {plan['Node Type']} operation materializes the result of its child node in memory to avoid re-computing the values."

#Unique
def unique_annotate(plan):
    """
    This function generates an annotation for the Unique operation. 

    :param plan:
    :return Annotation for Unique node type:
    """
    return f"The {plan['Node Type']} operation takes sorted input and eliminates adjacent duplicates."

#Merge Join
def merge_join_annotate(plan):
    """
    This function generates an annotation for the Merge Join operation. 
    Different annotations are generated based on the 'Node Type', 'Join Type' and 'Merge Cond' node attributes in the 
    query plan

    :param plan:
    :return Annotation for Merge Join node type:
    """
    annotation = f"The {plan['Node Type']} operation joins the resulting children already sorted by their shared join key"

    # Get the merge condition and remove unnecessary strings
    if "Merge Cond" in plan:
        annotation += " with condition " + plan["Merge Cond"].replace("::text", "")


    # Check the join type
    if "Join Type" == "Semi":
        annotation += " but the result only contains the records from the left relation"

    annotation += "."

    return annotation

#SetOp
def setop_annotate(plan):
    """
    This function generates an annotation for the SetOp operation. 
    Different annotations are generated based on the 'Node Type' and 'Command' node attributes in the 
    query plan

    :param plan:
    :return Annotation for SetOp node type:
    """
    annotation = f"The {plan['Node Type']} operation looks for the "

    # SQL 'Except' command
    if str(plan["Command"]) == "Except" or str(plan["Command"]) == "Except All":
        annotation += "differences"

    # SQL 'Intercept' command
    else:
        annotation += "similarities"

    annotation += " in records between the two tables scanned in the previous operation."

    return annotation


#Subquery Scan
def subquery_scan_annotate(plan):
    """
    This function generates an annotation for the Subquery Scan operation. 

    :param plan:
    :return Annotation for Subquery Scan node type:
    """
    return f"The {plan['Node Type']} operation scans the output of a sub-query in the range table."


#Values Scan
def values_scan_annotate(plan):
    """
    This function generates an annotation for the Values Scan operation. 

    :param plan:
    :return Annotation for Values Scan node type:
    """
    return f"The {plan['Node Type']} operation scans the literal VALUES clause."

#Seq Scan
def seq_scan_annotate(plan):
    """
    This function generates an annotation for the Seq Scan operation. 
    Different annotations are generated based on the 'Node Type', 'Relation Name', 'Alias' and 'Filter' node attributes 
    in the query plan

    :param plan:
    :return Annotation for Seq Scan node type:
    """
    annotation = f"The {plan['Node Type']} operation does a scan on "

    if "Relation Name" in plan:
        annotation += plan["Relation Name"]

    if "Alias" in plan:
        if plan["Relation Name"] != plan["Alias"]:
            annotation += f" which has an alias of {plan['Alias']}"

    if "Filter" in plan:
        annotation += f" and then filters it with the conditions: {plan['Filter'].replace('::text', '')}"

    annotation += "."

    return annotation

#Nested Loop
def nested_loop_annotate(plan):
    """
    This function generates an annotation for the Nested Loop operation. 

    :param plan:
    :return Annotation for Nested Loop node type:
    """
    return f"The {plan['Node Type']} operation iterates through all the rows in the inner table for each row in the outer table and see if they match the join condition."

#Sort
def sort_annotate(plan):
    """
    This function generates an annotation for the Sort operation. 
    Different annotations are generated based on the 'Node Type' and 'Sort Key' node attributes in the 
    query plan

    :param plan:
    :return Annotation for Sort node type:
    """
    annotation = (
        f"The {plan['Node Type']} operation will sort the results "
    )

    # If the specified sort key is DESC
    if "DESC" in plan["Sort Key"]:
        annotation += (
                str(plan["Sort Key"].replace("DESC", ""))
                + " in descending order"
        )

    # If the specified sort key is INC
    elif "INC" in plan["Sort Key"]:
        annotation += (
                str(plan["Sort Key"].replace("INC", ""))
                + " in increasing order"
        )

    # Otherwise specify the attribute
    else:
        annotation += f"by {str(plan['Sort Key'])}"

    annotation += "."

    return annotation

#Hash
def hash_annotate(plan):
    """
    This function generates an annotation for the Hash operation. 

    :param plan:
    :return Annotation for Hash node type:
    """
    return f"The {plan['Node Type']} operation reads data into a hash table, where it can easily be looked up by the hash key."

#Hash Join
def hash_join_annotate(plan):
    """
    This function generates an annotation for the Hash Join operation. 
    
    :param plan:
    :return Annotation for Hash Join node type:
    """
    annotation = f"The {plan['Node Type']} operation will join the resulting tuples from the previous operations using a hash {plan['Join Type']} {'Join'}"

    # Get the hash condition and remove unnecessary strings
    if "Hash Cond" in plan:
        annotation += f" based on the condition: {plan['Hash Cond'].replace('::text', '')}"

    annotation += "."

    return annotation

# Default
def default_annotate(plan):
    """
    This function generates a default  annotation for any operation. 

    :param plan:
    :return Annotation for any type:
    """
    return f"The {plan['Node Type']} operation is performed."


class Annotate(object):
    """
    The Annotate class maps the query plan to the specific node type to generate the respective annotation
    """

    annotation_dict = {
        "Aggregate": aggregate_annotate,
        "Append": append_annotate,
        "CTE Scan": cte_annotate,
        "Function Scan": function_scan_annotate,
        "Group": group_annotate,
        "Gather Merge": gather_merge_annotate,
        "Index Scan": index_scan_annotate,
        "Index Only Scan": index_only_scan_annotate,
        "Limit": limit_annotate,
        "Materialize": materialize_annotate,
        "Unique": unique_annotate,
        "Merge Join": merge_join_annotate,
        "SetOp": setop_annotate,
        "Subquery Scan": subquery_scan_annotate,
        "Values Scan": values_scan_annotate,
        "Seq Scan": seq_scan_annotate,
        "Nested Loop": nested_loop_annotate,
        "Sort": sort_annotate,
        "Hash": hash_annotate,
        "Hash Join": hash_join_annotate
    }

def NodeAnnotation(node,alt_node,alt_cost):
    """
    The NodeAnnotation function generates the complete annotation for a node operation, including the explanation and 
    the alternative node types and costs to show that the node type has the lowest cost. 

    :param node, alt_cost, alt_node:
    :return Annotation for the node operation:
    """
    annotation = ''
    annotation = f" This query is implemented by performing a {node['Node Type']} operation." \
        f"\n {Annotate().annotation_dict.get(node['Node Type'],default_annotate)(node)} "
    if 'Scan' in node['Node Type'] or 'Join' in node['Node Type'] or 'Loop' in node['Node Type']:
        # annotation += f" placeholder"
        if len(alt_node) == 1:
            annotation += f"\n The {alt_node[0]} operations were not used as it costs {'{:.2f}'.format(alt_cost[0] - node['Total Cost'])} more."
        if len(alt_node)>=2 and len(alt_cost)>=2:
            annotation += f"\n The {alt_node[0]} and {alt_node[1]} operations were not used as it costs {'{:.2f}'.format(alt_cost[0] - node['Total Cost'])} and {'{:.2f}'.format(alt_cost[1] - node['Total Cost'])} more."
    return annotation



def MapAnnotation(parsed_query,postgres_tree,alt_node,alt_cost):
    """
    The MapAnnotation function generates the mapping from the different sql query types to the various nodes generated from the query plan.
    This function returns a dictionary of the different sql query types involved as the key and the the values are the respective Node annotations involved for these sql 
    statements.The alternative nodes and the respective cost differences are also included in this annotation as the NodeAnnotation function mentioned above is used here.
    The different query types are:
    {
    “select” : {sql statements in the select clause} ,
	“from”:{sql statements sql in the from clause},
	“from_join”:{sql statements in the from clause which are used for joins},
	“where_join”:{sql statements in the where clause that are used for joins},
	“where_filter”:{sql statements in the where clause used for filtering values},
	“group_by”:{sql statements in the group by clause},
	“order_by”:{sql statements in the order by clause},
	“limit”:{sql statements in the limit clause},
	“subqueries”:{sql statements that are due to subqueries},
    }

    :parsed_query, postgres_tree, alt_node, alt_cost:
    :return mapped_queries 
    """
    mapped_queries = {}
    if(("Join Type" in postgres_tree.keys())):
        for i in parsed_query["from_join"]:
            join_condition = i.split('=')
            if("Join Filter" in postgres_tree.keys() and join_condition[0].strip() in postgres_tree["Join Filter"] and join_condition[1][1:-1] in postgres_tree["Join Filter"]):
                if "from_join" not in mapped_queries.keys():
                    mapped_queries["from_join"] = {}
                mapped_queries["from_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            elif('Merge Cond' in postgres_tree.keys() and join_condition[0].strip() in postgres_tree['Merge Cond'] and join_condition[1][1:-1] in postgres_tree['Merge Cond']):
                if "from_join" not in mapped_queries.keys():
                    mapped_queries["from_join"] = {}
                mapped_queries["from_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            elif('Hash Cond' in postgres_tree.keys() and join_condition[0].strip() in postgres_tree['Hash Cond'] and join_condition[1][1:-1] in postgres_tree['Hash Cond']):
                if "from_join" not in mapped_queries.keys():
                    mapped_queries["from_join"] = {}
                mapped_queries["from_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            else:
                if "from_join" not in mapped_queries.keys():
                    mapped_queries["from_join"] = {}
                if postgres_tree['Node Type'] == 'Nested Loop':
                    mapped_queries["from_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                    break

        for i in parsed_query["where_join"]:

            join_condition = i.split('=')
            if("Join Filter" in postgres_tree.keys() and join_condition[0].strip() in postgres_tree["Join Filter"] and join_condition[1][1:-1] in postgres_tree["Join Filter"]):
                if "where_join" not in mapped_queries.keys():
                    mapped_queries["where_join"] = {}
                mapped_queries["where_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            elif('Merge Cond' in postgres_tree.keys() and join_condition[0].strip() in postgres_tree['Merge Cond'] and join_condition[1][1:-1] in postgres_tree['Merge Cond']):
                if "where_join" not in mapped_queries.keys():
                    mapped_queries["where_join"] = {}
                mapped_queries["where_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            elif('Hash Cond' in postgres_tree.keys() and join_condition[0].strip() in postgres_tree['Hash Cond'] and join_condition[1][1:-1] in postgres_tree['Hash Cond']):
                if "where_join" not in mapped_queries.keys():
                    mapped_queries["where_join"] = {}
                mapped_queries["where_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                break
            else:
                if "where_join" not in mapped_queries.keys():
                    mapped_queries["where_join"] = {}
                if postgres_tree['Node Type'] == 'Nested Loop':
                    mapped_queries["where_join"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                    break


    if('Scan' in postgres_tree["Node Type"]):
        if 'from' not in mapped_queries.keys():
            mapped_queries['from'] = {}
        mapped_queries['from'][postgres_tree['Relation Name']] = NodeAnnotation(postgres_tree,alt_node,alt_cost)

    if "Filter" in postgres_tree.keys():
        for i in parsed_query["where_filter"]:
            join_condition = i.split(' ')

            if('.' in join_condition):
                if type(postgres_tree['Filter']) == list:
                    for j in postgres_tree['Filter']:
                        if "where_filter" not in mapped_queries.keys():
                            mapped_queries["where_filter"] = {}
                        mapped_queries["where_filter"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                        for cond in join_condition:
                            if '.' in cond:
                                if cond.split('.')[1][1:-1] not in j:
                                    mapped_queries["where_filter"].pop(i,None)
                            else:
                                if cond not in j:
                                    mapped_queries["where_filter"].pop(i,None)
                else:
                    j = postgres_tree['Filter']
                    if "where_filter" not in mapped_queries.keys():
                        mapped_queries["where_filter"] = {}
                    mapped_queries["where_filter"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                    for cond in join_condition:
                        if '.' in cond:
                            if cond.split('.')[1] not in j:
                                mapped_queries["where_filter"].pop(i, None)
                        else:
                            if cond not in j:
                                mapped_queries["where_filter"].pop(i, None)
            else:
                if type(postgres_tree['Filter']) == list:
                    for j in postgres_tree['Filter']:
                        if "where_filter" not in mapped_queries.keys():
                            mapped_queries["where_filter"] = {}
                        mapped_queries["where_filter"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                        for cond in join_condition:
                            if cond not in j:
                                mapped_queries["where_filter"].pop(i, None)
                else:
                    j = postgres_tree['Filter']
                    if "where_filter" not in mapped_queries.keys():
                        mapped_queries["where_filter"] = {}
                    mapped_queries["where_filter"][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
                    for cond in join_condition:
                        if cond not in j:
                            mapped_queries["where_filter"].pop(i, None)

    if "Group Key" in postgres_tree.keys():
        for i in parsed_query['group_by']:
            if type(postgres_tree['Group Key']) == list:
                for j in postgres_tree['Group Key']:
                    if(i in j):
                        if 'group_by' not in mapped_queries.keys():
                            mapped_queries['group_by'] = {}
                        mapped_queries['group_by'][i] = NodeAnnotation(postgres_tree,alt_node,alt_cost)
            else:
                j = postgres_tree['Group Key']
                if(i in j):
                    if 'group_by' not in mapped_queries.keys():
                        mapped_queries['group_by'] = {}
                    mapped_queries['group_by'][i] = NodeAnnotation(postgres_tree, alt_node, alt_cost)

    return mapped_queries


