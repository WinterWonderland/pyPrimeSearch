from PySide2.QtWidgets import QMainWindow
from PySide2.QtUiTools import QUiLoader
from PySide2.QtCore import QMetaObject


class UiLoader(QUiLoader):
    _baseinstance = None

    def createWidget(self, classname, parent=None, name=''):
        if parent is None and self._baseinstance is not None:
            widget = self._baseinstance
        else:
            widget = super(UiLoader, self).createWidget(classname, parent, name)
            if self._baseinstance is not None:
                setattr(self._baseinstance, name, widget)
        return widget

    def loadUi(self, uifile, baseinstance=None):
        self._baseinstance = baseinstance
        widget = self.load(uifile)
        QMetaObject.connectSlotsByName(widget)
        return widget
    
            
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        UiLoader().loadUi(r"main_window.ui", self)
        
    def set_block_size(self, block_size, lower_limit):
        self.label_blocksize.setText("block size: {0}  (will be regulated to get the prefered block time. minimum {1})".format(block_size, lower_limit))
        
    def set_block_time(self, block_time, prefered_block_time):
        self.label_blocktime.setText("block time: {0:.3f}s  (will be regulated to {1}s)".format(block_time, prefered_block_time))
        
    def set_prime_count(self, prime_count):
        self.label_primecount.setText("prime count: {0}".format(prime_count))
        
    def set_max_prime(self, max_prime):
        self.label_maxprime.setText("max prime: {0}".format(max_prime))
        
    def set_ui_update_interval(self, interval, sleep_factor, lower_limit):
        self.label_uiupdateinterval.setText("ui update interval: {0:.1f}s  ({1}x time of the ui update. minimum {2}s)".format(interval, sleep_factor, lower_limit))
