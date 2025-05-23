import os
import gi
import cv2
import numpy as np
import threading
import queue
import time
import sys
import multiprocessing as mp

from datetime import datetime
from utils import ThreadSafeCounter

def get_time():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("当前时间（毫秒）：", current_time)

os.environ['NO_PROXY'] = '*'
gi.require_version('Gst', '1.0')
from gi.repository import Gst,GLib

Gst.init(None)


class RTSPStream:
    def __init__(self, rtsp_url,mq,ctrl,use_hw_decode=True, reconnect_max=5):
        self.rtsp_url = rtsp_url
        self.use_hw_decode = use_hw_decode
        self.reconnect_max = reconnect_max
        self.pipeline = None
        self.appsink = None
        self.running = False

        self.mq = mq
        self.ctrl = ctrl
        #
        self.reconnect_attempts = ThreadSafeCounter()  # 新增：重连计数器
        self.error_received = False   # 新增：错误标志
        self.last_frame_time = None   # 新增：最后帧时间戳
        self.initial_connection = True# 新增：初始连接阶段标志

    def bus_call(self, bus, message):
        msg_type = message.type
        if msg_type == Gst.MessageType.EOS:
            print("[BUS] End of Stream")
            self.loop.quit()

        elif msg_type == Gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            print(f"[BUS WARNING] {err}: {debug}")

        elif msg_type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print(f"[BUS ERROR] {err}: {debug}")

            self.loop.quit()
        return True

    def build_pipeline(self):
        if self.use_hw_decode:
            decode = "rtph264depay ! h264parse ! nvv4l2decoder ! videoconvert"
        else:
            decode = "rtph264depay ! avdec_h264 ! videoconvert"
        pipeline_str = (
            f'rtspsrc location={self.rtsp_url} latency=20 buffer-mode=none protocols=tcp ! '
            f'{decode} !videoscale ! video/x-raw,format=BGR,width=[1,1280],height=[1,720],pixel-aspect-ratio=1/1  ! appsink name=appsink sync=false max-buffers=1 drop=true'
        )
        print("构建GStreamer管道：", pipeline_str)
        return Gst.parse_launch(pipeline_str)
    
    def get_image(self):
        start_time = time.time()
        initial_connection_timeout = 10  # 初始连接超时时间
        while self.running:
            if self.initial_connection and (time.time()-start_time > initial_connection_timeout):
                print("错误：初始连接超时")
                self.running = False

            if self.last_frame_time and (time.time()-self.last_frame_time > 5):
                print("警告：帧接收超时")
                self.running = False

            sample = self.appsink.emit('try-pull-sample', Gst.SECOND // 10 )
            if sample:
                buf = sample.get_buffer()
                caps = sample.get_caps()
                arr = self.gst_buffer_to_ndarray(buf, caps)
                if arr is not None:
                    try:
                        self.mq.put(arr, timeout=1)
                    except queue.Full:
                        print ("queue full")
                        pass
                self.last_frame_time = time.time()
                if self.initial_connection:
                    self.initial_connection = False
            else:
                # 没有帧，稍等
                #print ("没有帧，稍等")
                time.sleep(0.01)

        self.loop.quit()

    def start(self):
        t_stop = threading.Thread(target=self.stop)
        t_stop.start()
        while self.reconnect_attempts.get_value() < self.reconnect_max:
            self.reconnect_attempts.increment()
            print (self.reconnect_attempts.get_value(),"次连接")
            self.running = True
            self.pipeline = self.build_pipeline()
            
            self.appsink = self.pipeline.get_by_name('appsink')
            self.appsink.set_property('emit-signals', False)
            self.loop = GLib.MainLoop()
            bus = self.pipeline.get_bus()
            bus.add_signal_watch()
            bus.connect ("message", self.bus_call)

            t_stream = threading.Thread(target=self.get_image)
            t_stream.start()

            print("Starting pipeline \n")
            self.pipeline.set_state(Gst.State.PLAYING)
            try:
                self.loop.run()
            except:
                pass
            self.running = False
            t_stream.join()
            self.pipeline.set_state(Gst.State.NULL)
            print("Stop pipeline \n")
            
            reconnect_attempts = self.reconnect_attempts.get_value()
            if reconnect_attempts < self.reconnect_max:
                print(f"尝试重连 ({reconnect_attempts}/{self.reconnect_max})")

        self.mq.put(None)  # 确保主线程退出
        t_stop.join()

    @staticmethod
    def gst_buffer_to_ndarray(buf, caps):
        try:
            width = caps.get_structure(0).get_value('width')
            height = caps.get_structure(0).get_value('height')
            success, map_info = buf.map(Gst.MapFlags.READ)
            if not success:
                return None
            data = np.frombuffer(map_info.data, np.uint8)
            arr = data.reshape((height, width, 3))
            buf.unmap(map_info)
            return arr
        except Exception as e:
            print("解码为ndarray失败:", e)
            return None

    def stop(self):
        while not self.ctrl.is_set():
            time.sleep(0.001)
        self.reconnect_attempts.add(self.reconnect_max)
        self.running = False
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
        print("已停止RTSP拉流")

if __name__ == "__main__":
    get_time()
    rtsp_url = "rtsp://115.190.104.1:8554/202505230000001"
    use_hw_decode = False

    frame_queue = mp.Queue(maxsize=10)
    ctrl = mp.Event()

    stream = RTSPStream(rtsp_url, frame_queue,ctrl,use_hw_decode=use_hw_decode, reconnect_max=5)
    t_stream = mp.Process(target=stream.start)
    t_stream.start()
    cnt = 0

    try:
        while True:
            frame = frame_queue.get()
            if frame is None:
                break
            if cnt == 0:
                print ("出图了")
                get_time()
            cnt +=1
            cv2.imshow('RTSP', frame)
            key = cv2.waitKey(1)
            if key == 27:
                break
        #cv2.destroyAllWindows()
    except KeyboardInterrupt:
        pass
    finally:

        ctrl.set()
        frame_queue.put(None)

    t_stream.join()
