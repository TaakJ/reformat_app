import sys
from pathlib import Path
from os.path import join
from datetime import datetime
import webbrowser
from time import sleep
from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QFileDialog,
    QGroupBox,
    QRadioButton,
    QPushButton,
    QGridLayout,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLineEdit,
    QLabel,
    QDateEdit,
    QProgressBar,
    QCheckBox,
    QComboBox,
)
from PyQt6.QtCore import (
    QThread,
    QObject,
    pyqtSignal,
)
from qt_material import apply_stylesheet
from .setup import Folder, PARAMS, CONFIG
from .main import StartApp

class Jobber(QObject):
    set_total_progress = pyqtSignal(int)
    set_current_progress = pyqtSignal(int)
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.results = None

    def run(self):
        func = StartApp()
        self.results = func.results
        for i in range(1,11):
            self.set_current_progress.emit(int(i * 10))
            sleep(0.1)

        self.finished.emit()

class setup_app(QWidget):
    def __init__(self, app):
        super().__init__()

        self.__thread = QThread()

        if PARAMS["manual"] is False:
            StartApp()
        else:
            self.ui()
            sys.exit(app.exec())

    def ui(self):

        self.all_module = PARAMS["source"]
        self.filename = {module: f'MANUAL_{CONFIG[module]["output_file"]}' for module in self.all_module}
        self.module = self.all_module

        grid = QGridLayout()
        grid.addWidget(self.layout1(),0,0)
        grid.addWidget(self.layout2(),0,1)
        grid.addWidget(self.layout3(),1,0,1,2)
        grid.addWidget(self.layout4(),2,0)
        grid.addWidget(self.layout5(),2,1)
        self.setLayout(grid)

        self.setWindowTitle("App")
        self.setGeometry(700,200,620,400)
        self.show()

    def layout1(self):
        self.groupbox1 = QGroupBox("RUN EACH MODULE")

        hbox1 = QHBoxLayout()
        self._all = QCheckBox("ALL")
        self._all.setChecked(True)
        self._all.setEnabled(True)
        hbox1.addWidget(QLabel("Module :"))
        hbox1.addWidget(self._all)

        hbox2 = QHBoxLayout()
        self.combobox = QComboBox()
        self.combobox.addItems(self.module)
        self.combobox.setDisabled(True)
        hbox2.addWidget(QLabel("Select Module:"))
        hbox2.addWidget(self.combobox)

        hbox3 = QHBoxLayout()
        self.calendar = QDateEdit()
        self.calendar.setCalendarPopup(True)
        self.today = datetime.today()
        self.calendar.setDisplayFormat("yyyy-MM-dd")
        self.calendar.calendarWidget().setSelectedDate(self.today)
        hbox3.addWidget(QLabel("Select Date:"))
        hbox3.addWidget(self.calendar)

        vbox = QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(hbox3)
        self.groupbox1.setLayout(vbox)

        self._all.clicked.connect(self.task_all_checked)
        self.combobox.activated.connect(self.task_select_module)

        return self.groupbox1

    def layout2(self):
        self.groupbox2 = QGroupBox("Specify Output file")

        hbox1 = QHBoxLayout()
        self.radio1 = QRadioButton("Overwrite")
        self.radio1.setChecked(True)
        radio2 = QRadioButton("Create new")
        self.mode = "overwrite"
        hbox1.addWidget(self.radio1)
        hbox1.addWidget(radio2)

        hbox2 = QHBoxLayout()
        self.tmp_checked = QCheckBox("Generate Tmp file")
        self.tmp_checked.setCheckable(True)
        self.tmp_checked.setChecked(True)
        # self.clear_tmp = QCheckBox("Clear Tmp file")
        # self.clear_tmp.setCheckable(True)
        # self.clear_tmp.setChecked(False)
        hbox2.addWidget(self.tmp_checked)
        # hbox2.addWidget(self.clear_tmp)

        vbox1 = QVBoxLayout()
        self.mode_label = QLabel("e.g. MANUAL_{module}.csv")
        vbox1.addWidget(self.mode_label)

        vbox = QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(vbox1)

        self.groupbox2.setLayout(vbox)
        self.radio1.clicked.connect(self.task_select_mode)
        radio2.clicked.connect(self.task_select_mode)

        return self.groupbox2

    def layout3(self):
        self.groupbox3 = QGroupBox("Directory Each module")

        self.get_value = self.combobox.currentText()
        defualt_input_dir = CONFIG[self.get_value]["input_dir"]
        defualt_output_dir = CONFIG[self.get_value]["output_dir"]

        input_lable = QLabel("Incoming Path :")
        self.input_dir = QLineEdit()
        self.input_dir.setText(defualt_input_dir)
        self.input_dir.setEnabled(False)
        self.input_dir.setReadOnly(True)
        self.input_btn = QPushButton("Download")
        self.input_btn.setEnabled(False)

        output_lable = QLabel("Outgoing Path :")
        self.output_dir = QLineEdit()
        self.output_dir.setText(defualt_output_dir)
        self.output_dir.setEnabled(False)
        self.output_dir.setReadOnly(True)
        self.output_btn = QPushButton("Upload")
        self.output_btn.setEnabled(False)

        layout = QGridLayout()
        layout.addWidget(input_lable,0,0)
        layout.addWidget(self.input_dir,0,1)
        layout.addWidget(self.input_btn,0,2)

        layout.addWidget(output_lable,2,0)
        layout.addWidget(self.output_dir,2,1)
        layout.addWidget(self.output_btn,2,2)
        self.groupbox3.setLayout(layout)

        self.input_btn.clicked.connect(lambda: self.task_open_dialog(1))
        self.output_btn.clicked.connect(lambda: self.task_open_dialog(2))

        return self.groupbox3

    def layout4(self):

        self.groupbox4 = QGroupBox("RUN AND STATUS JOB")

        vbox1 = QVBoxLayout()
        self.progress = QProgressBar()
        self.progress.setStyleSheet(
                                    """QProgressBar {
                                        color: #000;
                                        border: 2px solid grey;
                                        border-radius: 5px;
                                        text-align: center;}
                                    QProgressBar::chunk {
                                        background-color: #a5c6ff;
                                        width: 10px;
                                        margin: 0.5px;}
                                        """
        )
        self.label = QLabel("Press the button to start job.")
        self.run_btn = QPushButton("START")
        self.run_btn.setFixedSize(110,40)
        vbox1.addWidget(self.progress)
        vbox1.addWidget(self.label)
        vbox1.addWidget(self.run_btn)
        vbox1.addStretch(1)

        vbox = QVBoxLayout()
        vbox.addLayout(vbox1)
        vbox.addStretch(1)
        self.groupbox4.setLayout(vbox)

        self.run_btn.clicked.connect(self.task_run_job)

        return self.groupbox4

    def layout5(self):
        self.groupbox5 = QGroupBox("log file")

        vbox1 = QVBoxLayout()
        self.time_label = QLabel("No Output.")
        vbox1.addWidget(self.time_label)

        hbox = QHBoxLayout()
        self._success_log = QPushButton("success")
        self._success_log.setFixedSize(110,40)
        self._success_log.setHidden(True)
        self._error_log = QPushButton("error")
        self._error_log.setHidden(True)
        self._error_log.setFixedSize(110,40)
        hbox.addWidget(self._success_log)
        hbox.addWidget(self._error_log)
        hbox.addStretch(1)

        vbox = QVBoxLayout()
        vbox.addLayout(vbox1)
        vbox.addLayout(hbox)
        vbox.addStretch(1)

        self.groupbox5.setLayout(vbox)

        self._success_log.clicked.connect(lambda: self.task_open_log(1))
        self._error_log.clicked.connect(lambda: self.task_open_log(2))

        return self.groupbox5

    def task_all_checked(self):
        if self._all.isChecked():
            self.combobox.setDisabled(True)
            self.input_dir.setEnabled(False)
            self.output_dir.setEnabled(False)
            self.input_btn.setEnabled(False)
            self.output_btn.setEnabled(False)
            ## select all module
            self.module = self.all_module
        else:
            self.combobox.setDisabled(False)
            self.input_dir.setEnabled(True)
            self.output_dir.setEnabled(True)
            self.input_btn.setEnabled(True)
            self.output_btn.setEnabled(True)
            ## select each module
            self.module = [self.combobox.currentText()]

    def task_select_module(self):
        self.get_value = self.combobox.currentText()
        self.module = [self.get_value]
        self.input_dir.setText(CONFIG[self.get_value]["input_dir"])
        self.output_dir.setText(CONFIG[self.get_value]["output_dir"])

    def task_select_mode(self):
        if self.radio1.isChecked():
            self.mode_label.setText("e.g. MANUAL_{module}.csv")
            self.mode = "overwrite"
        else:
            self.mode_label.setText("e.g. MANUAL_{module}_YYYYYMMDD.csv")
            self.mode = "new"

    def task_open_dialog(self,event):
        if event == 1:
            key_dir = "input_dir"
            default_dir = self.input_dir
        else:
            key_dir = "output_dir"
            default_dir = self.output_dir

        self.get_value = self.combobox.currentText()
        select_dir = CONFIG[self.get_value][key_dir]

        dialog = QFileDialog()
        dir_name = dialog.getExistingDirectory(
            parent=self,
            caption="Select a directory",
            directory=select_dir,
            options=QFileDialog.Option.ShowDirsOnly,
        )
        if dir_name:
            CONFIG[self.get_value][key_dir] = join(Path(dir_name))
            default_dir.setText(dir_name)

    def task_open_log(self,event):
        date = datetime.now().strftime("%Y%m%d")
        log_dir = join(Folder.LOG,date)
        if event == 1:
            open_log = join(log_dir,"_success.log")
        else:
            open_log = join(log_dir,"_error.log")
        webbrowser.open(open_log)

    def task_run_job(self):
        self.progress.reset()
        self.label.setText("Job is running...")
        self.time_label.setHidden(True)
        self._success_log.setHidden(True)
        self._error_log.setHidden(True)

        PARAMS.update(
            {
                "source": self.module,
                "batch_date": self.calendar.calendarWidget().selectedDate().toPyDate(),
                "store_tmp": self.tmp_checked.isChecked(),
                "write_mode": self.mode,
            }
        )

        for module in self.module:
            if module in self.filename.keys() and self.mode == "new":
                suffix = PARAMS["batch_date"].strftime("%Y%m%d")
                CONFIG[module]["output_file"] = f"{Path(self.filename[module]).stem}_{suffix}.csv"
            else:
                CONFIG[module]["output_file"] = self.filename[module]

        if not self.__thread.isRunning():
            self.__thread = self.__get_thread()
            self.__thread.start()

    def __get_thread(self):
        thread = QThread()
        tasks = Jobber()
        tasks.moveToThread(thread)

        thread.worker = tasks
        thread.started.connect(tasks.run)
        tasks.finished.connect(thread.quit)

        tasks.set_total_progress.connect(self.progress.setMaximum)
        tasks.set_current_progress.connect(self.progress.setValue)
        tasks.finished.connect(lambda x=tasks.results: self.task_job_finished(tasks.results))

        return thread

    def task_job_finished(self,results):
        self.progress.setValue(self.progress.maximum())
        self.time_label.setText(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.time_label.setHidden(False)
        self._success_log.setHidden(False)

        if "Uncompleted" in [completed_task.result()["task"] for completed_task in results[0]]:
            self.label.setText("Job has errored. Please see log file!")
            self._error_log.setHidden(False)
        else:
            self.label.setText("Job has been succeed.")

def main():
    app = QApplication(sys.argv)
    apply_stylesheet(
        app,
        theme="light_blue.xml",
        extra={
            "font_family": "monoespace",
            "density_scale": "0",
            "pyside6": True,
            "linux": True,
        },
    )
    setup_app(app)