import psycopg2  # need install
import json
from annotation import *
from pprint import pprint

class DatabaseCursor:
    #Init the DB
    def __init__(self):
        with open("dbconfig.json", "r") as file:
            self.config = json.load(file)

    #Get all the tables from the DB
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

        schema = {}
        for item in response:
            attrs = schema.get(item[0], [])
            attrs.append(item[1])
            schema[item[0]] = attrs
        # log schema
        print("Database schema as follow: ")
        for t, table in enumerate(schema):
            print(t + 1, table, schema.get(table))
        return schema

    #Gets a single plan with defined constraints
    def getplan(self, query, constraintsdict):
        #Connect to DB
        self.conn = psycopg2.connect(
            host=self.config["host"],
            dbname=self.config["dbname"],
            user=self.config["user"],
            password=self.config["pwd"],
            port=self.config["port"]
        )
        self.cur = self.conn.cursor()

        #SET the contraints before querying
        for constrain in constraintsdict:
            self.cur.execute(f"SET {constrain} TO {constraintsdict[constrain]}")

        #Run the explain query & get the output
        print(f"query: EXPLAIN (FORMAT JSON, BUFFERS, SUMMARY, VERBOSE, SETTINGS) {query}")
        self.cur.execute(f"EXPLAIN (FORMAT JSON, BUFFERS, SUMMARY, VERBOSE, SETTINGS) {query}")
        response = self.cur.fetchall()[0][0][0]['Plan']
        self.conn.close()

        return response

    def getallplans(self, query):
        allplans = []
        planscost = {}

        #Have all constraints turned on.
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

        #Get the QEP first
        print("Getting chosenplan")
        chosenplan = self.getplan(query, constraintsdict)
        allplans.append(chosenplan)
        print(f"Chosenplan: {chosenplan}")

        #Decide what to disable from QEP
        listtochange = []
        self.decidewhattochange(listtochange, chosenplan, planscost, True)
        print(f'listtochange: {listtochange}')

        #While the list of constraints to change, keep looping
        while len(listtochange) >= 1:
            item = listtochange.pop()
            #Turn off the constraint
            constraintsdict[item] = "off"
            #Get the plan
            plan = self.getplan(query, constraintsdict)
            #print(f"Alternate Plan: {plan}")
            #Decide what to change again
            self.decidewhattochange(listtochange, plan, planscost, False)
            allplans.append(plan)

        return allplans, planscost

    def decidewhattochange(self, listtochange, masterplan, cost, bestplan=True, first=False):
        jointables = []

        if "Plans" in masterplan:
            for plan in masterplan["Plans"]:
                temp = self.decidewhattochange(listtochange, plan, cost, bestplan)
                jointables.append(temp)

        if bestplan == True and not 'tables' in cost:
            cost["tables"] = {}

        if masterplan["Node Type"] == 'Seq Scan':
            table = masterplan["Relation Name"]
            name = masterplan["Alias"]
            if bestplan == True:
                cost["tables"][table] = "Seq Scan"
            print(f'Reach SeqScan Table: {table} Name: {name}')
            return table

        elif masterplan["Node Type"] == 'Index Scan':
            table = masterplan["Relation Name"]
            name = masterplan["Alias"]
            if bestplan == True:
                cost["tables"][table] = "Index Scan"
            print(f'Reach IndexScan Table: {table} Name: {name}')
            return table

        elif masterplan["Node Type"] == 'Hash':
            print(f'Reach Hash: {jointables[0]}')
            return jointables[0]

        elif masterplan["Node Type"] == 'Hash Join':
            condition = masterplan['Hash Cond']
            if bestplan == True:
                cost[condition] = {'bestplan': 'Hash Join'}
                cost[condition]['cost'] = {}
            cost[condition]['cost']['Hash Join'] = masterplan['Total Cost']
            if not 'enable_hashjoin' in listtochange:
                listtochange.append('enable_hashjoin')
            print(f'Reach Hash Join: {jointables[0]} & {jointables[1]} using {condition}')
            return f'hj_it_{jointables[0]}_{jointables[1]}'

        elif masterplan["Node Type"] == 'Merge Join':
            condition = masterplan['Merge Cond']
            if not condition in cost.keys():
                print(f"Cannot find condition, likely need swap")
                cond = condition.lstrip('(').rstrip(')')
                condarr = cond.split('=')
                condition = f"({condarr[1].strip()} = {condarr[0].strip()})"
            if bestplan == True:
                cost[condition] = {'bestplan': 'Merge Join'}
                cost[condition]['cost'] = {}
            cost[condition]['cost']['Merge Join'] = masterplan['Total Cost']
            if not 'enable_mergejoin' in listtochange:
                listtochange.append('enable_mergejoin')
            print(f'Reach Merge Join: {jointables[0]} & {jointables[1]} using {condition}')
            return f'mj_it_{jointables[0]}_{jointables[1]}'

        elif masterplan["Node Type"] == 'Nested Loop':
            condition = self.getnestedloopcond(masterplan)
            if not condition in cost.keys():
                print(f"Cannot find condition, likely need swap")
                cond = condition.lstrip('(').rstrip(')')
                condarr = cond.split('=')
                condition = f"({condarr[1].strip()} = {condarr[0].strip()})"
            if bestplan == True:
                cost[condition] = {'bestplan': 'Nested Loop'}
                cost[condition]['cost'] = {}
            cost[condition]['cost']['Nested Loop'] = masterplan['Total Cost']
            print(f'Reach Nested Loop: {jointables[0]} & {jointables[1]} using {condition}')
            return f'nl_it_{jointables[0]}_{jointables[1]}'

        else:
            print(f'Reach Else Masterplan: {masterplan}')

    #Special method to get join condition for nested loop
    def getnestedloopcond(self, plan):
        if "Plans" in plan:
            for plan in plan["Plans"]:
                temp = self.getnestedloopcond(plan)
                if not temp == None:
                    return temp

        #print(f'getnlcond plan: {plan}')
        #print(f'plan["Node Type"]: {plan["Node Type"]} and {plan.keys()}')
        if plan["Node Type"] == 'Index Scan' and 'Index Cond' in plan.keys():
            #print(f"index cond here getnested plan: {plan}")
            #print(f"index cond: {plan['Index Cond']}")
            return plan['Index Cond']
        else:
            return




