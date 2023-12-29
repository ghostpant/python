# -*- coding: utf-8 -*-
import heapq
import time
import random


from oslo_log import log
from monotonic import monotonic as _time
from oslo_service import loopingcall
from eventlet import greenthread
from eventlet import queue
from apollo.common import comdef
from apollo.common import common


LOG = log.getLogger(__name__)
EMPTY_LOOP_WAIT_TIME = comdef.TASK_QUERY_PERIOD_MID
NORMAL_TIMEOUT = comdef.SYNC_PROGRESS_INTERVAL
REMAIN_TASKS_DELAY_TIME = comdef.TASK_QUERY_PERIOD_MID


class _Item(object):
    def __init__(self, value, delay):
        assert delay >= 0, "delay can not be less than 0"
        self._value = value
        # 延时时间增加1s内的随机数，以免爆发式创建协程更新进度
        self._available_at = _time() + delay + round(random.uniform(0, 1), 3)

    @property
    def value(self):
        return self._value

    @property
    def available_at(self):
        return self._available_at

    def __cmp__(self, another):
        if not isinstance(another, self.__class__):
            raise TypeError("expect %s, not %s" %
                            (self.__class__.__name__, type(another).__name__))
        if self.available_at < another.available_at:
            return -1
        elif self.available_at == another.available_at:
            return 0
        else:
            return 1


class DelayQueue(queue.Queue):

    def __init__(self, maxsize):
        super(DelayQueue, self).__init__(maxsize)
        # 用于去重，等任务执行完成之后才会remove，而不是出队
        self.set_item = set()
        # 用于返回默认空的元素
        self._default_in_delay_item = _Item(None, 0)

    def _has_available_item(self):
        if self.qsize() == 0:
            return False
        item = self.queue[0]
        if item.available_at <= _time():
            return True
        else:
            return False

    def _init(self, maxsize):
        self.queue = []

    def _put(self, item):
        assert isinstance(item, (list, tuple)) and \
            len(item) == 2 and \
            isinstance(item[1], (int, long, float)) and item[0] is not None, \
            "item should be tuple, value cannot be null" \
            "and the second element should be float"
        item = _Item(item[0], item[1])
        heapq.heappush(self.queue, item)

    def _get(self):
        u"""
        eg:
        item.value = {"driver_task_id":"xxx", "action":"xxx"}
        item.available_at = 1703506257.25
        尝试获取, 判断是否有超过延时时间的元素,没有则返回None
        """
        if not self._has_available_item():
            return self._default_in_delay_item
        item = heapq.heappop(self.queue)
        return item

    def remove(self, item_value):
        task_id = item_value.get("task_id", None)
        try:
            self.set_item.remove(task_id)
        except KeyError:
            # 如果不在队列不抛出异常
            return

    def get_after_delay(self, block=True, timeout=None):
        try:
            item = self.get(block=block, timeout=timeout)
        except queue.Empty:
            raise
        else:
            return item.value

    def push(self, item, block=True, timeout=None):
        try:
            self.put(item, block=block, timeout=timeout)
            # 去重
            task_id = item[0].get("task_id", None)
            self.set_item.add(task_id)
        except queue.Full:
            LOG.warning("push failed, task queue is full!")
        except Exception:
            LOG.exception("push failed, wait for next time!")


class TaskListener(object):
    # 监听延时队列内的任务，并处理
    def __init__(self, task_queue, do_func, thread_pool):
        self.queue = task_queue
        self.do_func = do_func
        self.task_pool = thread_pool

    def _listen_queue(self):
        u"""
        持续监听队列内容，如果有task则执行对应函数
        服务启动时，一直阻塞监听，直到有元素进入队列
        value为None，认为队首的元素未达到延时时间，并没有出队，sleep2s后再次尝试获取
        否则
        """
        _block = False
        _wait_time = None
        while True:
            try:
                _value = self.queue.get_after_delay(block=_block,
                     
