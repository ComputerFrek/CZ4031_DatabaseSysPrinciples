import psycopg2  # need install
import json
from annotation import *

class DatabaseCursor:
    def __init__(self):
        with open("dbconfig.json", "r") as file:
            self.config = json.load(file)

    #def __enter__(self):
    #    self.conn = psycopg2.connect(
    #        host=self.config["host"],
    #        dbname=self.config["dbname"],
    #        user=self.config["user"],
    #        password=self.config["pwd"],
    #        port=self.config["port"]
    #    )
    #    self.cur = self.conn.cursor()
        # self.cur.execute("SET search_path TO " + self.config['schema'])
    #    return self.cur

    #def __exit__(self, exc_type, exc_val, exc_tb):
    #    # some logic to commit/rollback
    #    self.conn.close()

    def getschema(self):
        self.conn = psycopg2.connect(
            host=self.config["host"],
            dbname=self.config["dbname"],
            user=self.config["user"],
            password=self.config["pwd"],
            port=self.config["port"]
        )
        self.cur = self.conn.cursor()

        query = "SELECT table_name, column_name, data_type, character_maximum_length as length FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position"
        self.cur.execute(query)
        response = self.cur.fetchall()

        self.conn.close()

        # parse response as dictionary
        schema = {}
        for item in response:
            # cols are table_name, column_name, data_type, length (nullable)
            attrs = schema.get(item[0], [])
            attrs.append(item[1])
            schema[item[0]] = attrs
        # log schema
        print("Database schema as follow: ")
        for t, table in enumerate(schema):
            print(t + 1, table, schema.get(table))
        return schema

    def getplan(self, query, constraintsdict):
        self.conn = psycopg2.connect(
            host=self.config["host"],
            dbname=self.config["dbname"],
            user=self.config["user"],
            password=self.config["pwd"],
            port=self.config["port"]
        )
        self.cur = self.conn.cursor()

        for constrain in constraintsdict:
            #print(f"constrain: {constrain} value: {constraintsdict[constrain]}")
            self.cur.execute(f"SET {constrain} TO {constraintsdict[constrain]}")

        print(f"query: EXPLAIN (FORMAT JSON, BUFFERS, VERBOSE, SETTINGS) {query}")
        self.cur.execute(f"EXPLAIN (FORMAT JSON, BUFFERS, VERBOSE, SETTINGS) {query}")
        #response = self.cur.fetchall()[0][0][0]
        response = self.cur.fetchall()[0][0][0]['Plan']
        self.conn.close()

        #print(f"qep: {response}")
        #plan_annotated = Annotator().wrapper(response)
        #print(f"annotated qep: {plan_annotated}")
        #self.window.setResult(plan_annotated)

        return response

    def getallplans(self, query):
        allplans = []

        constraintsdict = {
            "enable_async_append": "on",
            "enable_bitmapscan": "on",
            "enable_gathermerge": "on",
            "enable_hashagg": "on",
            "enable_hashjoin": "on",
            "enable_incremental_sort": "on",
            "enable_indexscan": "on",
            "enable_indexonlyscan": "on",
            "enable_material": "on",
            "enable_memoize": "on",
            "enable_mergejoin": "on",
            "enable_nestloop": "on",
            "enable_parallel_append": "on",
            "enable_parallel_hash": "on",
            "enable_partition_pruning": "on",
            "enable_partitionwise_join": "on",
            "enable_partitionwise_aggregate": "on",
            "enable_seqscan": "on",
            "enable_sort": "on",
            "enable_tidscan": "on"
        }

        print("Getting chosenplan")
        chosenplan = self.getplan(query, constraintsdict)
        allplans.append(chosenplan)
        print(f"Chosenplan: {chosenplan}")

        self.decidewhattochange(chosenplan)

        #print("getallplan2")
        #constraintsdict["enable_hashjoin"] = "off"
        #allplans.append(self.getplan(query, constraintsdict))
        return allplans

    def decidewhattochange(self, masterplan, first=False):
        listtochange = []

        print(f'decide1')
        if "Plans" in masterplan:
            print(f"decide2: {masterplan}")
            for plan in masterplan["Plans"]:
                print(f"recursive call: {plan}")
                temp = self.decidewhattochange(plan)

        if masterplan["Node Type"] == 'Seq Scan':
            table = masterplan["Relation Name"]
            name = masterplan["Alias"]
            print(f'Reach SeqScan Table: {table} Name: {name}')
            return
        elif masterplan["Node Type"] == 'Index Scan':
            table = masterplan["Relation Name"]
            name = masterplan["Alias"]
            print(f'Reach IndexScan Table: {table} Name: {name}')
            return
        else:
            print(f'Reach Else Masterplan: {masterplan}')

    def analyzeplan(self, plan, head=False):
        print("analyzeplan1")
        if "Plan" in plan or "Plans" in plan:
            print("analyzeplan2")
            for item in plan["Plans"]:
                print("analyzeplan3")
                #child = self.analyzeplan(item)
                print(f'item: {item} value: {plan["Plans"]["item"]}')

