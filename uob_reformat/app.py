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
    Qt
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
        for i in range(1, 11):
            self.set_current_progress.emit(int(i * 10))
            sleep(0.05)

        self.finished.emit()

class setup_app(QWidget):
    def __init__(self, app):
        super().__init__()

        self.__thread = QThread()

        if PARAMS['manual'] is False:
            StartApp()
        else:
            self.ui()
            sys.exit(app.exec())

    def ui(self):

        self.all_module = PARAMS['source']
        self.module = self.all_module

        grid = QGridLayout()
        grid.addWidget(self.layout1(), 0, 0)
        grid.addWidget(self.layout2(), 0, 1)
        grid.addWidget(self.layout3(), 1, 0, 1, 2)
        grid.addWidget(self.layout4(), 2, 0)
        grid.addWidget(self.layout5(), 2, 1)
        self.setLayout(grid)

        self.setWindowTitle("App")
        self.setGeometry(700, 200, 600, 400)
        self.show()

    def layout1(self):
        self.groupbox1 = QGroupBox("Run each module")

        hbox1 = QHBoxLayout()
        self._all = QCheckBox("All")
        self._all.setChecked(True)
        self._all.setEnabled(True)
        hbox1.addWidget(QLabel("Module"))
        hbox1.addWidget(self._all)

        hbox2 = QHBoxLayout()
        self.combobox = QComboBox()
        self.combobox.addItems(self.module)
        self.combobox.setDisabled(True)
        hbox2.addWidget(QLabel("Select module"))
        hbox2.addWidget(self.combobox)

        hbox3 = QHBoxLayout()
        self.calendar = QDateEdit()
        self.calendar.setCalendarPopup(True)
        self.today = datetime.today()
        self.calendar.setDisplayFormat("yyyy-MM-dd")
        self.calendar.calendarWidget().setSelectedDate(self.today)
        hbox3.addWidget(QLabel("Select date"))
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
        self.groupbox2 = QGroupBox("Specify output file")

        hbox1 = QHBoxLayout()
        radio1 = QRadioButton("Create new file")
        self.radio2 = QRadioButton("Overwrite file")
        self.radio2.setChecked(True)
        self.mode = "overwrite"
        hbox1.addWidget(self.radio2)
        hbox1.addWidget(radio1)
        
        hbox2 = QHBoxLayout()
        self.tmp_checked = QCheckBox("Temporary file")
        self.tmp_checked.setChecked(False)
        self.backup_checked = QCheckBox("Backup file")
        self.backup_checked.setChecked(True)
        hbox2.addWidget(self.backup_checked)
        hbox2.addWidget(self.tmp_checked)
        
        vbox = QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)

        self.groupbox2.setLayout(vbox)
        radio1.clicked.connect(self.task_select_mode)
        self.radio2.clicked.connect(self.task_select_mode)

        return self.groupbox2

    def layout3(self):
        self.groupbox3 = QGroupBox("Directory each module")

        self.get_value = self.combobox.currentText()
        defualt_input_dir = CONFIG[self.get_value]["input_dir"]
        defualt_output_dir = CONFIG[self.get_value]["output_dir"]

        input_lable = QLabel("Incoming path")
        self.full_input = QLineEdit()
        self.full_input.setText(defualt_input_dir)
        self.full_input.setEnabled(False)
        self.full_input.setReadOnly(True)
        self.input_btn = QPushButton("Download")
        self.input_btn.setEnabled(False)

        output_lable = QLabel("Outgoing path")
        self.output_dir = QLineEdit()
        self.output_dir.setText(defualt_output_dir)
        self.output_dir.setEnabled(False)
        self.output_dir.setReadOnly(True)
        self.output_btn = QPushButton("Upload")
        self.output_btn.setEnabled(False)
        
        self.checked1 = QCheckBox("Execute Application")
        self.checked1.setChecked(True)
        self.checked2 = QCheckBox("Execute Paramlist")
        self.checked2.setChecked(True)
        self.select_files = [1,2]
        
        layout = QGridLayout()
        layout.addWidget(input_lable, 0, 0)
        layout.addWidget(self.full_input, 0, 1)
        layout.addWidget(self.input_btn, 0, 2)

        layout.addWidget(output_lable, 2, 0)
        layout.addWidget(self.output_dir, 2, 1)
        layout.addWidget(self.output_btn, 2, 2)
        
        layout.addWidget(self.checked1, 4, 1, alignment=Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(self.checked2, 4, 1, alignment=Qt.AlignmentFlag.AlignRight)
        
        self.groupbox3.setLayout(layout)

        self.input_btn.clicked.connect(lambda: self.task_open_dialog(1))
        self.output_btn.clicked.connect(lambda: self.task_open_dialog(2))
        self.checked1.clicked.connect(self.task_select_execute)
        self.checked2.clicked.connect(self.task_select_execute)

        return self.groupbox3

    def layout4(self):

        self.groupbox4 = QGroupBox("Run and status job")

        vbox1 = QVBoxLayout()
        self.progress = QProgressBar()
        self.progress.setStyleSheet("""QProgressBar {
                                        color: #000;
                                        border: 2px solid grey;
                                        border-radius: 5px;
                                        text-align: center;}
                                    QProgressBar::chunk {
                                        background-color: #a5c6ff;
                                        width: 10px;
                                        margin: 0.5px;}
                                        """)
        self.label = QLabel("Press the button to start job.")
        self.run_btn = QPushButton("START")
        self.run_btn.setFixedSize(110, 40)
        vbox1.addWidget(self.progress)
        vbox1.addWidget(self.label, alignment=Qt.AlignmentFlag.AlignCenter)
        vbox1.addWidget(self.run_btn, alignment=Qt.AlignmentFlag.AlignCenter)
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
        self.time_label = QLabel("No output.")
        vbox1.addWidget(self.time_label)

        hbox = QHBoxLayout()
        self.status_log = QPushButton("status")
        self.status_log.setFixedSize(110, 40)
        self.status_log.setHidden(True)
        self.error_log = QPushButton("error")
        self.error_log.setHidden(True)
        self.error_log.setFixedSize(110, 40)
        hbox.addWidget(self.status_log)
        hbox.addWidget(self.error_log)
        hbox.addStretch(1)

        vbox = QVBoxLayout()
        vbox.addLayout(vbox1)
        vbox.addLayout(hbox)
        vbox.addStretch(1)

        self.groupbox5.setLayout(vbox)

        self.status_log.clicked.connect(lambda: self.task_open_log(1))
        self.error_log.clicked.connect(lambda: self.task_open_log(2))

        return self.groupbox5

    def task_all_checked(self):
        if self._all.isChecked():
            self.combobox.setDisabled(True)
            self.full_input.setEnabled(False)
            self.output_dir.setEnabled(False)
            self.input_btn.setEnabled(False)
            self.output_btn.setEnabled(False)
            ## select all module
            self.module = self.all_module
        else:
            self.combobox.setDisabled(False)
            self.full_input.setEnabled(True)
            self.output_dir.setEnabled(True)
            self.input_btn.setEnabled(True)
            self.output_btn.setEnabled(True)
            ## select each module
            self.module = [self.combobox.currentText()]

    def task_select_module(self):
        self.get_value = self.combobox.currentText()
        self.module = [self.get_value]
        self.full_input.setText(CONFIG[self.get_value]['input_dir'])
        self.output_dir.setText(CONFIG[self.get_value]['output_dir'])

    def task_select_mode(self):
        if self.radio2.isChecked():
            self.mode = "overwrite"
        else:
            self.mode = "new"

    def task_open_dialog(self, event):
        if event == 1:
            key_dir = "input_dir"
            default_dir = self.full_input
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
            options=QFileDialog.Option.ShowDirsOnly)
        if dir_name:
            CONFIG[self.get_value][key_dir] = join(Path(dir_name))
            default_dir.setText(dir_name)
            
    def task_select_execute(self):
        state_chk1 = self.checked1.isChecked()
        state_chk2 = self.checked2.isChecked()
        
        if state_chk1 is True and state_chk2 is True:
            self.select_files = [1,2]
        elif state_chk1 is True and state_chk2 is False:
            self.select_files = [1]
        elif state_chk1 is False and state_chk2 is True:
            self.select_files = [2]
        else:
            self.checked1.setChecked(True)
            self.select_files =  [1]
        
    def task_open_log(self, event):
        
        date = datetime.now().strftime("%Y%m%d")
        log_dir = join(Folder.LOG, date)
        
        if event == 1:
            log_file = join(log_dir, "log_status.log")
        else:
            log_file = join(log_dir, "log_error.log")
        webbrowser.open(log_file)

    def task_run_job(self):
        self.progress.reset()
        self.label.setText("Job is running...")
        self.time_label.setHidden(True)
        self.status_log.setHidden(True)
        self.error_log.setHidden(True)
        
        PARAMS.update({
                'source': self.module,
                'batch_date': self.calendar.calendarWidget().selectedDate().toPyDate(),
                'store_tmp': self.tmp_checked.isChecked(),
                'write_mode': self.mode,
                'select_files': self.select_files,
                'backup': self.backup_checked.isChecked()
            })
        
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

    def task_job_finished(self, results):
        self.progress.setValue(self.progress.maximum())
        self.time_label.setText(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.time_label.setHidden(False)
        self.status_log.setHidden(False)
        
        if "Uncompleted" in [completed_task.result()["task"] for completed_task in results[0]]:
            self.label.setText("Job has errored. Please see log file!")
            self.error_log.setHidden(False)
        else:
            self.label.setText("Job has been succeed.")

def main():
    app = QApplication(sys.argv)
    apply_stylesheet(
        app,
        invert_secondary=True,
        theme="light_blue.xml",
        extra={
            "font_family": "monoespace",
            "density_scale": "0",
            "pyside6": True,
            "linux": True,
        },)
    setup_app(app)
