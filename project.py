import sys
from PyQt5.QtWidgets import QApplication
from qt_material import apply_stylesheet
from interface import *
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
        self.window = AppGUI()
        self.window.setOnDatabaseChanged(lambda: self.onDatabaseChanged())
        self.window.btnExecOnClick(lambda: self.analyseQuery())

    def run(self):
        self.window.show()
        list_db = list(self.DatabaseCur.config.keys())
        print(f"JSON FILE DB CONFIG: {list_db}")
        self.window.setListDatabase(list_db)
        sys.exit(self.app.exec_())

    def onDatabaseChanged(self):
        # check cur database, update schema?
        cur_db = self.window.list_database.currentText()
        print(f"DB Selected: {cur_db}")
        self.DatabaseCur.config = self.DatabaseCur.config[cur_db]
        self.updateSchema()

    def hasDbConfig(self):
        if self.DatabaseCur.config == None:
            return False
        return True

    def analyseQuery(self):
        self.window.clearQEPQuery()
        if not self.hasDbConfig():
            self.window.showError("Cannot find DB config")
            return
        try:
            query = self.window.getQueryInput()
            if not query:
                self.window.showError("Invalid SQL Query", None)
                return

            response = self.DatabaseCur.getallplans(query)

            i = 0
            for item in response[0]:
                print(f"Plan {i}: {item}")
                i = i + 1

            #pprint(response[1], indent=2)

            annotationstring = Annotator().annotate(response[1])
            self.window.setAnalysisResult(annotationstring)
        except Exception as e:
            #print(f'Error: {str(e)}, ErrorType: {type(e)}')
            self.window.showError(f'Error: {str(e)}, ErrorType: {type(e)}')

    def updateSchema(self):
        if not self.hasDbConfig():
            self.window.setSchema(None)
            self.window.showError("Unable to find DB Config")
            return
        try:
            self.window.setSchema(self.DatabaseCur.getschema())
        except Exception as e:
            print(str(e))
            self.window.showError(f'Error: {str(e)}, ErrorType: {type(e)}')

if __name__ == "__main__":
    Program().run()
