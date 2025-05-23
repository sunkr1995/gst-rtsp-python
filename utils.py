import threading

class ThreadSafeCounter:
    def __init__(self, initial_value=0):
        self._value = initial_value
        self._lock = threading.Lock()  # 使用可重入锁保证线程安全

    def increment(self):
        """原子性加一操作"""
        with self._lock:  
            self._value += 1

    def decrement(self):
        """原子性减一操作"""
        with self._lock:
            self._value -= 1

    def get_value(self):
        """安全获取当前值"""
        with self._lock:  
            return self._value

    def reset(self, new_value=0):
        """重置计数器"""
        with self._lock:
            self._value = new_value
    
    def add(self,x):
        with self._lock:
            self._value = self._value + x
    
    def sub(self,x):
        with self._lock:
            self._value = self._value - x
