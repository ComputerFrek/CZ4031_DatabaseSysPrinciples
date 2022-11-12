from pprint import pprint
class Annotator:
    #Annotation method, reading from plancost dict
    def annotate(self, plancost):
        annresult = ""

        for x in plancost['tables']:
            if plancost['tables'][x] == 'Seq Scan':
                annresult += f"Table {x} read using {plancost['tables'][x]} as there is no index\n"
            elif plancost['tables'][x] == 'Index Scan':
                annresult += f"Table {x} read using {plancost['tables'][x]}\n"
            else:
                annresult += f"Table {x} read using {plancost['tables'][x]}\n"

        annresult += "\n"

        for x in plancost:
            if x == "tables":
                continue
            annresult += f"Join {x} is using {plancost[x]['bestplan']} due to lowest cost.\n"
            annresult += "Cost of joins:\n"
            for y in plancost[x]['cost']:
                annresult += f"{y} -> {plancost[x]['cost'][y]}\n"
            annresult += "\n"

        return annresult






