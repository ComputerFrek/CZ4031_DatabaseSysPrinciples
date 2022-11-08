from PyQt5 import uic
from PyQt5.QtWidgets import *

class AppGUI(QMainWindow):
    def __init__(self):
        super(AppGUI, self).__init__()

        uic.loadUi("interface.ui", self)

        # locate widgets
        self.in_query = self.findChild(QTextEdit, "in_query")
        self.out_query = self.findChild(QLabel, "out_query")
        self.btn_exec = self.findChild(QPushButton, "btn_exec")
        self.btn_clear = self.findChild(QPushButton, "btn_clear")
        self.list_database = self.findChild(QComboBox, "combo_databases")

        # attach signals to widgets
        self.btn_clear.clicked.connect(self.clear)
        self.list_database.currentIndexChanged.connect(self._onDatabaseChanged)

    
    def AddNewTab(self,tabname):
        """
        tabname: using Table name
        Usage: Create a new Tab using QWidget() for Db Table.
        """

        newTab = QWidget()
        newTab.layout = QVBoxLayout()

        listCntr = QListWidget()

        for attr in self.schema[tabname]:
            listCntr.addItem(attr) #adding each table's column to QListWidget
        
        newTab.layout.addWidget(listCntr)
        newTab.setLayout(newTab.layout)
        self.parentTab.addTab(newTab,tabname) #adding new tab to Tab List

    def tabOnClick(self,index):
        """
        Usage: Signal for tab Onclick, and print out the selected Tab's Index.
        """
        self.parentTab.setCurrentIndex(index)

    def showError(self, errMessage, execption=None):
        """
        Usage: Popout window to display error message using QMessageBox().
        """
        msgBox = QMessageBox()
        msgBox.setWindowTitle("Exception Details")
        msgBox.setStyleSheet("QLabel{min-width: 450px;min-hight: 550px;}");

        msgBox.setText(errMessage) #display error msg

        #display more detailed error msg
        if execption is not None:
            msgBox.setDetailedText(str(execption))
        msgBox.setStandardButtons(QMessageBox.Ok)
        msgBox.exec_()

    def clear(self):
        """
        Usage: Reset clearInputQuery() and clearQEPQuery().
        """
        self.clearInputQuery()
        self.clearQEPQuery()

    def clearInputQuery(self):
        """
        Usage: Set var:in_query to empty string.
        """
        self.in_query.setPlainText("")

    def clearQEPQuery(self):
        """
        Usage: Set var:out_query to empty string.
        """
        self.out_query.setText("")

    def getQueryInput(self):
        """
        Usage: getter of var:in_query, that capture the input of user's SQL input.
        """
        return self.in_query.toPlainText()

    def setAnalysisResult(self, text):
        """
        Usage: setter of var:out_query to display the result to UI.
        """
        self.out_query.setText(text)

    def setSchema(self, schema=None):
        """
        Usage: getter of schema name and populate tables into Tabs.
        """

        # skip if schema is not available
        if schema is None:
            return

        self.parentTab = self.findChild(QTabWidget, "tabWidget")
        self.parentTab.tabBarClicked.connect(self.tabOnClick) #attach onclick to the widget

        self.schema = schema #set this for easily reference
        for table in self.schema:
            self.AddNewTab(table) #each db table

    def btnExecOnClick(self, callback):
        """
        Usage: Signal for btn_exec Onclick.
        """
        if callback:
            self.btn_exec.clicked.connect(callback)

    def setOnDatabaseChanged(self, callback):
        """
        Usage: Setter to trigger OnChange event for db.
        """
        self.callback_db_changed = callback

    def setListDatabase(self, list_db=["TPC-H"]):
        """
        Usage: Adding db name into db list for easy reference.
        """
        self.list_database.clear()
        self.list_database.addItems(list_db)

    def _onDatabaseChanged(self, cur_index):
        """
        Usage: Signal for db Onchange event.
        """
        if hasattr(self, "callback_db_changed"):
            self.callback_db_changed()




