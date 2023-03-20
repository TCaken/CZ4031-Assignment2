from flask import Flask, render_template, request, url_for, flash, redirect, send_file
import matplotlib.pyplot as plt
import networkx as nx
import os
import webbrowser
from preprocessing import *
import matplotlib
matplotlib.use('Agg')

#Default Setting for Database (Hosted on AWS)
HOST = "cz4031-db.c3eyqxhdkvkd.ap-southeast-1.rds.amazonaws.com"    # or "localhost"
PORT = 5432                                                         # Default 5342
DB = "postgres"                                                     # Database Name
USER = "postgres"                                                   # Username
PASSWORD = "password"                                               # Password

#Create a connection with Database
aws = Connection(HOST, PORT, DB, USER , PASSWORD)
schema = aws.get_schema_data()

queries = dict()
path = "queries"
for file in os.listdir(path):
    filename = f'{path}/{file}'
    
    with open(filename, 'r') as f:
        queries[file] = f.read()
    f.close()

#Flask Application
app = Flask(__name__)
app.config['SECRET_KEY'] = 'asdfghjklpoiuyt'
tree = None

#Main Page
@app.route("/", methods=['GET','POST'])
def main():
    '''
    First page in application. Page includes sections, like
    Query   = Place your query here, 
    Result  = Display annotation and hover event, 
    Plot    = Display the plot
    '''

    if request.method == 'POST':
        query = request.form.get('query')
        parsed_sql = SqlQuery(query)

        # Generate annotations based on query input
        # Passed dictionary as input for webpage
        try:
            query_plan = aws.explain_query_2(query)
            tree = Tree(query_plan, parsed_sql, query)
            tree.generate_altnodes(tree, aws.get_cur())
        except Exception as e:
            print(e)
            flash("Invalid Query Format!", category='error')
            return render_template("base.html", plot= 0 , schema=schema, queries=queries, sql = None)

        tree.get_networkx_info(tree.root)
        draw_graph(tree.nodesList, tree.edgeList)

        #Generate dictionary needed for annotation
        sql = {
            "select" : {},
            "from" : {},
            "from_join" : {},
            "where": {},
            "group_by" : {},
            "order_by" : {},
            "limit" : '',
            "subqueries" : {}
        }

        for section, list in parsed_sql.all.items():
            if section == 'select' or section == 'from':
                for alias, key in list.items():
                    sql[section][key] = [alias, ""]
            elif section == 'limit':
                sql[section] = list
            elif section == "where_filter" or section == "where_join" or section == "from_join":
                for key in list:
                    if key != "":
                        sql["where"][key] = ""
            else:
                for key in list:
                    if key != "":
                        sql[section][key] = ""

        for pair in tree.all_annotations:
            for keyword, queryDesc in pair.items():
                #print("KEY", keyword)
                for query, desc in queryDesc.items():
                    print(keyword, query, desc)
                    if keyword == 'select' or keyword == 'from':
                        sql[keyword][query][1] = desc
                    elif keyword == 'limit':
                        pass
                    elif keyword == "from_join":
                        sql[keyword][query] = desc
                    elif keyword == "where_filter" or keyword == "where_join":
                        sql["where"][query] = desc
                    elif keyword == "order_by" or keyword == 'group_by':
                        sql[keyword][query] = desc
                    else:
                        #Continue with subqueries
                        pass
                    #print("SQL", sql)
                
        return render_template("base.html", plot = 1, schema=schema, queries=queries, sql = sql)

    return render_template("base.html", plot= 0 , schema=schema, queries=queries, sql = None)


def topo_pos(G):
    """
    Generate Position in topological order, 
    with simple offsetting for visibility

    Keyword arguments:
    G -- Graph representative of tree nodes

    Returns:
    pos-dict -- Determines position of each nodes in graph
    """

    pos_dict = {}
    for i, node_list in enumerate(nx.topological_generations(G)):
        x_offset = len(node_list) / 2
        y_offset = 0.1
        for j, name in enumerate(node_list):
            pos_dict[name] = (j - x_offset, -i + j * y_offset)

    return pos_dict

def draw_graph(nodes, edges):
    """
    Draw a graph based on tree,
    with respective of its labels
    Savr the picture in static folder

    Keyword arguments:
    nodes -- Node List in tree
    edges -- Edge List from tree
    """
    G = nx.DiGraph()
    for node in nodes:
        G.add_node(node[0], cost=node[1], n_type=[node[2]],
                   alt_node=node[3], alt_cost=node[4])
    G.add_edges_from(edges)

    labels = {}
    for node in G:
        label = str(G.nodes[node]['n_type'][0]) + ' | Cost = ' + str(G.nodes[node]['cost']) + \
            '\n ALT node = ' + str(G.nodes[node]['alt_node']) + \
            ' | ALT cost = ' + str(G.nodes[node]['alt_cost'])
        labels[node] = label

    pos = topo_pos(G)
    plt.figure(figsize=(100, 80))
    plt.margins(0.5,0)
    nx.draw_networkx(G, pos, with_labels=True, font_weight='bold', font_size = 80, labels=labels)
    plt.savefig('static/graph.png',  format="PNG")
    