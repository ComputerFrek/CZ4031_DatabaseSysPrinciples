import sys
import time
import json
import psycopg2  # need install
from PyQt5.QtWidgets import QApplication  # need install
from qt_material import apply_stylesheet  # , list_themes # need install

import annotation
from interface import *
from demo import * #wj
from annotation import *
from preprocessing import *
from pprint import pprint

# extract hard coded values
FILE_APP_THEME = "dark_teal.xml"  # list_themes()[12]

class Program:
    def __init__(self):
        # init ui components
        self.DatabaseCur = DatabaseCursor()
        self.app = QApplication(sys.argv)
        apply_stylesheet(self.app, theme=FILE_APP_THEME)
        # self.window = UI() #O2
        self.window = demo() #wj 
        self.window.setOnDatabaseChanged(lambda: self.onDatabaseChanged())
        self.window.setOnAnalyseClicked(lambda: self.analyseQuery())

    def run(self):
        self.window.show()
        list_db = list(self.DatabaseCur.config.keys())
        print(f"List of database configs from json file: {list_db}")
        self.window.setListDatabase(list_db)
        sys.exit(self.app.exec_())

    def onDatabaseChanged(self):
        # check cur database, update schema?
        cur_db = self.window.list_database.currentText()
        print(f"Current selected database is {cur_db}")
        self.DatabaseCur.config = self.DatabaseCur.config[cur_db]
        self.updateSchema()

    def hasDbConfig(self):
        #if not hasattr(self, "db_config"):
        #    return False
        if self.DatabaseCur.config == None:
            return False
        return True

    def analyseQuery(self):
        self.window.clearQEPQuery()
        if not self.hasDbConfig():
            self.window.showError("Database configuration is not found")
            return
        try:
            query = self.window.readInput()
            if not query:
                print("query is empty")
                return

            response = self.DatabaseCur.getallplans(query)

            i = 0
            #print(f"Output:\n{response}")
            #pprint(response[0])
            for item in response[0]:
                print(f"Plan {i}: {item}")
                i = i + 1

            pprint(response[1], indent=2)

            annotationstring = Annotator().annotate2(response[1])
            self.window.setResult(annotationstring)

            #print("Comparing plans")
            #self.DatabaseCur.compareplans(response)
        except Exception as e:
            print(f'Error: {str(e)}, ErrorType: {type(e)}')
            self.window.showError("Unable to analyse query!", e)

    def updateSchema(self):
        if not self.hasDbConfig():
            self.window.setSchema(None)
            self.window.showError("Database configuration is not found")
            return
        try:
            self.window.setSchema(self.DatabaseCur.getschema())
        except Exception as e:
            print(str(e))
            self.window.showError("Unable to retrieve schema information!", e)


if __name__ == "__main__":
    Program().run()

