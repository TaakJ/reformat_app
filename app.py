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
    QCheckBox
)
from PyQt6.QtCore import (
    QThread, 
    QObject, 
    pyqtSignal
)
from qt_material import apply_stylesheet
from setup import setup_parser, setup_folder, setup_log, setup_config, Folder
from method import run_batch
from pathlib import Path
from os.path import join
import sys
import time
from datetime import datetime
import webbrowser

class Jobber(QObject):
    set_total_progress = pyqtSignal(int)
    set_current_progress = pyqtSignal(int)
    finished  = pyqtSignal()
    
    def __init__(self, params):
        super().__init__()
        self.param = params
        self.state = False

    def run(self):
        method = run_batch(self.param)
        self.state = method.state
        read_bytes = 0
        chunk_size = 1024
        if self.state:
            self.set_total_progress.emit(Path(join(Folder.EXPORT, Folder._FILE)).stat().st_size)
            while read_bytes <= Path(join(Folder.EXPORT, Folder._FILE)).stat().st_size:
                time.sleep(1)
                read_bytes += chunk_size
                self.set_current_progress.emit(read_bytes)
        else:
            self.set_total_progress.emit(100)
        self.finished.emit()
        
class setup_app(QWidget):
    def __init__(self):
        super().__init__()

        params = setup_parser().parsed_params
        setattr(params, 'config', setup_config())
        self.__thread = QThread()
        
        if params.manual is False:
            run_batch(vars(params))
        else:
            self.ui()
            sys.exit(app.exec())

    def ui(self):
        grid = QGridLayout()
        grid.addWidget(self.layout1(), 0, 0)
        grid.addWidget(self.layout2(), 0, 1)
        grid.addWidget(self.layout3(), 1, 0, 1, 2)
        grid.addWidget(self.layout4(), 2, 0)
        grid.addWidget(self.layout5(), 2, 1)
        self.setLayout(grid)

        self.setWindowTitle("App")
        self.setGeometry(650, 200, 700, 360)
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
        hbox1.addWidget(QLabel("Status Run:"))
        hbox1.addWidget(radio)

        hbox2 = QHBoxLayout()
        hbox2.addWidget(QLabel("Set Date:"))
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
        self.mode_lable = QLabel("e.g. manual_export.xlsx")
        self.checkbok = QCheckBox("Tmp file")
        self.checkbok.setCheckable(True)
        self.checkbok.setChecked(True)

        vbox = QVBoxLayout()
        vbox.addWidget(self.radio1)
        vbox.addWidget(radio2)
        vbox.addWidget(self.mode_lable)
        vbox.addWidget(self.checkbok)
        vbox.addStretch(1)
        self.groupbox2.setLayout(vbox)

        self.radio1.clicked.connect(self.select_mode)
        radio2.clicked.connect(self.select_mode)

        return self.groupbox2

    def layout3(self):
        self.groupbox3 = QGroupBox("Path.")
        self.groupbox3.setCheckable(True)
        self.groupbox3.setChecked(True)

        button1 = QPushButton("Browse")
        self.path1 = QLineEdit()
        self.path1.setText(Folder.RAW)
        self.path1.setReadOnly(True)

        button2 = QPushButton("Save")
        self.path2 = QLineEdit()
        self.path2.setText(Folder.EXPORT)
        self.path2.setReadOnly(True)

        layout = QGridLayout()
        layout.addWidget(QLabel("Input :"), 0, 0)
        layout.addWidget(self.path1, 0, 1)
        layout.addWidget(button1, 0, 2)

        layout.addWidget(QLabel("Output :"), 2, 0)
        layout.addWidget(self.path2, 2, 1)
        layout.addWidget(button2, 2, 2)
        self.groupbox3.setLayout(layout)

        button1.clicked.connect(lambda: self.open_dirs(Folder.RAW, 1))
        button2.clicked.connect(lambda: self.open_dirs(Folder.EXPORT, 2))

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
        
        self.run_btn.clicked.connect(self.run_job_tasks)

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
            
    def run_job_tasks(self):
        self.groupbox1.setChecked(False)
        self.groupbox2.setChecked(False)
        self.groupbox3.setChecked(False)
        self.groupbox4.setChecked(False)
        self.progress.reset()
        self.label.setText("Job is running...")
        self.time_label.setHidden(True)
        self.log.setHidden(True)
        self.file.setHidden(True)

        # set params for run job.
        batch_date = self.calendar.calendarWidget().selectedDate().toPyDate()
        mode = self.mode
        tmp = self.checkbok.isChecked()
        if self.mode == "overwrite":
            Folder._FILE = "manual_export.xlsx" 
        else:
            Folder._FILE = f"manual_export_{batch_date.strftime('%d%m%Y')}.xlsx"
        self.params = {
            "manual": True,
            "batch_date": batch_date,
            "store_tmp": tmp,
            "write_mode": mode,
        }
        if not self.__thread.isRunning():
            self.__thread = self.__get_thread()
            self.__thread.start()
        
    def __get_thread(self):
        thread = QThread()
        tasks = Jobber(self.params)
        tasks.moveToThread(thread)
        
        thread.worker = tasks
        thread.started.connect(tasks.run)
        tasks.finished.connect(thread.quit)
        
        tasks.set_total_progress.connect(self.progress.setMaximum)
        tasks.set_current_progress.connect(self.progress.setValue)
        tasks.finished.connect(
            lambda state=tasks.state: self.run_job_finished(tasks.state)
            )

        return thread

    def run_job_finished(self, state):
        self.groupbox1.setChecked(True)
        self.groupbox2.setChecked(True)
        self.groupbox3.setChecked(True)
        self.groupbox4.setChecked(True)
        self.progress.setValue(self.progress.maximum())
        self.time_label.setHidden(False)
        self.log.setHidden(False)
        self.time_label.setText(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if state:
            self.label.setText("Job has been succeed.")
            self.file.setHidden(False)
        else:
            self.label.setText("Job has been errored. Please check log file!")
    
    def select_mode(self):
        if self.radio1.isChecked():
            self.mode = "overwrite"
            self.mode_lable.setText("e.g. Export_manual.xlsx")
        else:
            self.mode = "new"
            self.mode_lable.setText("e.g. Export_manual_DDMMYYYY.xlsx")

    def open_dirs(self, select_path, event):
        dialog = QFileDialog()
        dir_name = dialog.getExistingDirectory(
            parent=self,
            caption="Select a directory",
            directory=select_path,
            options=QFileDialog.Option.ShowDirsOnly,
        )
        if dir_name: 
            if event == 1:
                Folder.RAW = join(Path(dir_name), '')
                self.path1.setText(Folder.RAW)
            else:
                Folder.EXPORT = join(Path(dir_name), '')
                self.path2.setText(Folder.EXPORT)
            
    def open_files(self, event):
        if event == 1:
            date = self.today.strftime("%d%m%Y")
            open_files =  join(Folder.LOG, f'log_{date}.log')
        else:
            open_files = join(Folder.EXPORT, Folder._FILE) 
        webbrowser.open(open_files)

if __name__ == "__main__":
    setup_folder()
    setup_log()
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
