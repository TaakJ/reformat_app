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
    Qt,
    QThread, 
    QObject, 
    pyqtSignal
)
from PyQt6.QtGui import (
    QFont
)
from qt_material import apply_stylesheet
from setup import setup_parser, setup_folder, setup_config, Folder
from method import start_app
from pathlib import Path
from os.path import join
import sys
import time
from datetime import datetime

# class Jobber(QObject):
#     set_total_progress = pyqtSignal(int)
#     set_current_progress = pyqtSignal(int)
#     finished  = pyqtSignal()
    
#     def __init__(self, params):
#         super().__init__()
#         self.param = params
#         self.state = False

#     def run(self):
        # method = run_batch(self.param)
        # method = start_app(self.param)
        # self.state = method.state
        # read_bytes = 0
        # chunk_size = 1024
        # if self.state:
        #     self.set_total_progress.emit(Path(join(Folder.EXPORT, Folder._FILE)).stat().st_size)
        #     while read_bytes <= Path(join(Folder.EXPORT, Folder._FILE)).stat().st_size:
        #         time.sleep(1)
        #         read_bytes += chunk_size
        #         self.set_current_progress.emit(read_bytes)
        # else:
        #     self.set_total_progress.emit(100)
        # self.finished.emit()
        
class setup_app(QWidget):
    def __init__(self):
        super().__init__()

        params = setup_parser().parsed_params
        setattr(params, "config", setup_config())
        # self.__thread = QThread()
        
        if params.manual is False:
            start_app(vars(params))
        else:
            self.ui(vars(params))
            sys.exit(app.exec())

    def ui(self, params):
        self._params = params
        self.module = self._params["source"]
        self.config  = self._params["config"]
        
        self.bold = QFont()
        self.bold.setBold(True)
        
        grid = QGridLayout()
        grid.addWidget(self.layout1(), 0, 0)
        grid.addWidget(self.layout2(), 0, 1)
        grid.addWidget(self.layout3(), 1, 0, 1, 2)
        # grid.addWidget(self.layout4(), 2, 0)
        # grid.addWidget(self.layout5(), 2, 1)
        self.setLayout(grid)

        self.setWindowTitle("App")
        self.setGeometry(650, 200, 600, 300)
        self.show()

    def layout1(self):
        self.groupbox1 = QGroupBox("Date.")
        self.groupbox1.setCheckable(True)
        self.groupbox1.setChecked(True)

        radio = QRadioButton("Manual")
        radio.setChecked(True)
        radio.setEnabled(False)

        self.calendar = QDateEdit()
        self.calendar.setCalendarPopup(True)
        self.today = datetime.today()
        self.calendar.setDisplayFormat("yyyy-MM-dd")
        self.calendar.calendarWidget().setSelectedDate(self.today)
        
        hbox1 = QHBoxLayout()
        label1 = QLabel("Status Run:")
        label1.setFont(self.bold)
        hbox1.addWidget(label1)
        hbox1.addWidget(radio)

        hbox2 = QHBoxLayout()
        label2 = QLabel("Set Date:")
        label2.setFont(self.bold)
        hbox2.addWidget(label2)
        hbox2.addWidget(self.calendar)
        
        vbox = QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addStretch(1)
        self.groupbox1.setLayout(vbox)

        return self.groupbox1

    def layout2(self):
        self.groupbox2 = QGroupBox("Mode.")
        self.groupbox2.setCheckable(True)
        self.groupbox2.setChecked(True)

        self.radio1 = QRadioButton("Overwrite file")
        self.radio1.setChecked(True)
        
        self.mode = "overwrite"
        radio2 = QRadioButton("New file")
        self.mode_label = QLabel("e.g. Manual_ADM.csv")
        self.mode_label.setFont(self.bold)
        
        self.checkbok = QCheckBox("Tmp file")
        self.checkbok.setCheckable(True)
        self.checkbok.setChecked(True)

        vbox = QVBoxLayout()
        vbox.addWidget(self.radio1)
        vbox.addWidget(radio2)
        vbox.addWidget(self.mode_label)
        vbox.addWidget(self.checkbok)
        vbox.addStretch(1)
        self.groupbox2.setLayout(vbox)

        self.radio1.clicked.connect(self.select_task)
        radio2.clicked.connect(self.select_task)

        return self.groupbox2

    def layout3(self):
        self.groupbox3 = QGroupBox("Directory.")
        self.groupbox3.setCheckable(True)
        self.groupbox3.setChecked(True)

        self.label1 = QLabel("Select :")
        self.label1.setFont(self.bold)
        
        self.combobox = QComboBox()
        self.combobox.addItems(self.module)
        
        self.label2 = QLabel("Input :")
        self.label2.setFont(self.bold)
        self.input_dir = QLineEdit()
        self.input_dir.setText(self.config["ADM"]["input_dir"])
        self.input_dir.setReadOnly(True)
        self.input_btn = QPushButton("Browse")
        
        self.label3 = QLabel("Output :")
        self.label3.setFont(self.bold)
        self.output_dir = QLineEdit()
        self.output_dir.setText(self.config["ADM"]["output_dir"])
        self.output_dir.setReadOnly(True)
        self.output_btn = QPushButton("Save")

        self.label4 = QLabel("File :   ")
        self.label4.setFont(self.bold)
        self.file_input = QLabel(f'{self.config["ADM"]["file"]}')
        
        layout = QGridLayout()
        layout.addWidget(self.label1, 0, 0)
        layout.addWidget(self.combobox, 0, 1)
        
        layout.addWidget(self.label2, 1, 0)
        layout.addWidget(self.input_dir, 1, 1)
        layout.addWidget(self.input_btn, 1, 2)
        
        layout.addWidget(self.label3, 2, 0)
        layout.addWidget(self.output_dir, 2, 1)
        layout.addWidget(self.output_btn, 2, 2)
        
        layout.addWidget(self.label4, 3, 0)
        layout.addWidget(self.file_input, 3, 1, 1 , 2)
        
        self.groupbox3.setLayout(layout)

        self.combobox.activated.connect(self.select_task)
        self.input_btn.clicked.connect(lambda: self.open_file_dialog(1))
        self.output_btn.clicked.connect(lambda: self.open_file_dialog(2))

        return self.groupbox3

    def layout4(self):
        self.groupbox4 = QGroupBox("Run Job.")
        self.groupbox4.setCheckable(True)
        self.groupbox4.setChecked(True)

        self.progress = QProgressBar()
        self.progress.setStyleSheet("""QProgressBar {
                                        color: #000;
                                        border: 2px solid grey;
                                        border-radius: 5px;
                                        text-align: center;}
                                    QProgressBar::chunk {
                                        background-color: #a5c6ff;
                                        width: 10px;
                                        margin: 0.5px;
                                    }"""
        )

        self.label = QLabel("Press the button to start job.")
        self.run_btn = QPushButton("START")
        self.run_btn.setFixedSize(90, 40)
        
        vbox = QVBoxLayout()
        vbox.addWidget(self.progress)
        vbox.addWidget(self.label)

        hbox = QHBoxLayout()
        hbox.addWidget(self.run_btn)
        hbox.addStretch(1)

        vbox.addLayout(hbox)
        vbox.addStretch(1)
        self.groupbox4.setLayout(vbox)
        
        # self.run_btn.clicked.connect(self.run_job_tasks)

        return self.groupbox4

    def layout5(self):
        groupbox = QGroupBox("Output.")
        groupbox.setFlat(True)
        self.log = QPushButton("Open Log")
        self.log.setHidden(True)
        self.file = QPushButton("Open File")
        self.file.setHidden(True)

        self.time_label = QLabel("No Output.")
        vbox = QVBoxLayout()
        vbox.addWidget(self.time_label)

        hbox = QHBoxLayout()
        hbox.addWidget(self.log)
        hbox.addWidget(self.file)
        hbox.addStretch(1)

        vbox.addLayout(hbox)
        vbox.addStretch(1)
        groupbox.setLayout(vbox)
        
        self.log.clicked.connect(lambda: self.open_files(1))
        self.file.clicked.connect(lambda: self.open_files(2))

        return groupbox
    
    def select_task(self):
        
        self.input_dir.setText(self.config[self.combobox.currentText()]["input_dir"])
        self.output_dir.setText(self.config[self.combobox.currentText()]["output_dir"])
        self.file_input.setText(self.config[self.combobox.currentText()]["file"])
        
        if self.radio1.isChecked():
            self.mode = "overwrite"
            self.mode_label.setText(f"e.g. Manual_{self.combobox.currentText()}.csv")
        else:
            self.mode = "new"
            self.mode_label.setText(f"e.g. Manual_{self.combobox.currentText()}-DDMMYY.csv")
            
            
    def open_file_dialog(self, event):
        
        if event == 1:
            _dir = "input_dir"
            browse =  self.input_dir
        else:
            _dir = "output_dir"
            browse =  self.output_dir
        select_dir = self.config[self.combobox.currentText()][_dir]
        
        dialog = QFileDialog()
        dir_name = dialog.getExistingDirectory(
            parent=self,
            caption="Select a directory",
            directory=select_dir,
            options=QFileDialog.Option.ShowDirsOnly)
        if dir_name: 
            self.config[self.combobox.currentText()][_dir] = join(Path(dir_name))
            browse.setText(dir_name)
            
    
    # def run_job_tasks(self):
    #     self.groupbox1.setChecked(False)
    #     self.groupbox2.setChecked(False)
    #     self.groupbox3.setChecked(False)
    #     self.groupbox4.setChecked(False)
    #     self.progress.reset()
    #     self.label.setText("Job is running...")
    #     self.time_label.setHidden(True)
    #     self.log.setHidden(True)
    #     self.file.setHidden(True)

    #     # set params for run job.
    #     batch_date = self.calendar.calendarWidget().selectedDate().toPyDate()
    #     mode = self.mode
    #     tmp = self.checkbok.isChecked()
    #     # if self.mode == "overwrite":
    #     #     ''
    #     #     # Folder._FILE = "manual_export.xlsx" 
    #     # else:
    #     #     ''
    #     #     # Folder._FILE = f"manual_export_{batch_date.strftime('%d%m%Y')}.xlsx"
    #     self.params = {
    #         "manual": True,
    #         "batch_date": batch_date,
    #         "store_tmp": tmp,
    #         "write_mode": mode,
    #     }
    #     if not self.__thread.isRunning():
    #         self.__thread = self.__get_thread()
    #         self.__thread.start()
        
    # def __get_thread(self):
    #     thread = QThread()
    #     tasks = Jobber(self.params)
    #     tasks.moveToThread(thread)
        
    #     thread.worker = tasks
    #     thread.started.connect(tasks.run)
    #     tasks.finished.connect(thread.quit)
        
    #     tasks.set_total_progress.connect(self.progress.setMaximum)
    #     tasks.set_current_progress.connect(self.progress.setValue)
    #     tasks.finished.connect(
    #         lambda state=tasks.state: self.run_job_finished(tasks.state)
    #         )

    #     return thread

    # def run_job_finished(self, state):
    #     self.groupbox1.setChecked(True)
    #     self.groupbox2.setChecked(True)
    #     self.groupbox3.setChecked(True)
    #     self.groupbox4.setChecked(True)
    #     self.progress.setValue(self.progress.maximum())
    #     self.time_label.setHidden(False)
    #     self.log.setHidden(False)
    #     self.time_label.setText(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    #     if state:
    #         self.label.setText("Job has been succeed.")
    #         self.file.setHidden(False)
    #     else:
    #         self.label.setText("Job has been errored. Please check log file!")
            
    # def open_files(self, event):
    #     if event == 1:
    #         date = self.today.strftime("%d%m%Y")
    #         ''
    #         # open_files =  join(Folder.LOG, f'log_{date}.log')
    #     else:
    #         ''
    #         # open_files = join(Folder.EXPORT, Folder._FILE) 
    #     # webbrowser.open(open_files)

if __name__ == "__main__":
    setup_folder()
    app = QApplication(sys.argv)
    apply_stylesheet(
        app,
        theme="light_blue.xml",
        invert_secondary=True,
        extra={
            "font_family": "Roboto",
        },
    )
    setup_app()
