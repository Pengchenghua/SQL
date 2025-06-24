# -*- coding: utf-8 -*-
import time
from functools import wraps
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import logging
import json
import csx_package_api.logger_api as logger_api

# 全局任务锁
task_lock = threading.RLock()
# 任务状态文件锁
file_lock = threading.Lock()
TASK_STATUS_FILE = "task_status.json"

class TaskManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler({
            'apscheduler.executors.default': {
                'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                'max_workers': 3  # 控制最大并发任务数
            },
            'apscheduler.job_defaults.coalesce': True,
            'apscheduler.job_defaults.max_instances': 1,
            'apscheduler.job_defaults.misfire_grace_time': 300,
            'apscheduler.job_defaults.timeout': 3600  # 任务超时时间(秒)
        })
        self.logger = logger_api.setup_logging()
        self.running_tasks = set()
        self._init_task_status_file()
        
    def _init_task_status_file(self):
        """初始化任务状态文件"""
        with file_lock:
            try:
                with open(TASK_STATUS_FILE, 'r') as f:
                    pass
            except FileNotFoundError:
                with open(TASK_STATUS_FILE, 'w') as f:
                    json.dump({}, f)
        
    def task_lock_decorator(self, func):
        """增强版任务锁装饰器(双重锁机制)"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 内存锁检查
            with task_lock:
                task_id = f"{func.__name__}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                if func.__name__ in self.running_tasks:
                    self.logger.warning(f"任务冲突阻止: {func.__name__} 已在执行中")
                    return None
                    
                # 文件锁检查
                with file_lock:
                    with open(TASK_STATUS_FILE, 'r') as f:
                        status = json.load(f)
                        if func.__name__ in status and status[func.__name__] == "running":
                            self.logger.warning(f"任务冲突阻止(文件锁): {func.__name__} 已在执行中")
                            return None
                            
                    # 更新任务状态
                    with open(TASK_STATUS_FILE, 'w') as f:
                        status[func.__name__] = "running"
                        json.dump(status, f)
                
                self.running_tasks.add(func.__name__)
                try:
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    elapsed = time.time() - start_time
                    self.logger.info(f"任务 {func.__name__} 执行完成, 耗时: {elapsed:.2f}秒")
                    return result
                except Exception as e:
                    self.logger.error(f"任务 {func.__name__} 执行失败: {str(e)}")
                    raise
                finally:
                    self.running_tasks.remove(func.__name__)
                    # 更新任务状态为完成
                    with file_lock:
                        with open(TASK_STATUS_FILE, 'r') as f:
                            status = json.load(f)
                        status[func.__name__] = "completed"
                        with open(TASK_STATUS_FILE, 'w') as f:
                            json.dump(status, f)
        return wrapper

    def scheduler_listener(self, event):
        """调度器事件监听"""
        if event.code == EVENT_JOB_ERROR:
            self.logger.error(f"任务执行失败: {event.job_id}", exc_info=event.exception)
        elif event.code == EVENT_JOB_MISSED:
            self.logger.warning(f"任务错过执行: {event.job_id}")

    def add_task(self, task_func, task_name, priority=5, **trigger_args):
        """添加定时任务(支持优先级)
        
        Args:
            task_func: 任务函数
            task_name: 任务名称
            priority: 任务优先级(1-10, 数字越小优先级越高)
            trigger_args: 触发器参数
        """
        decorated_task = self.task_lock_decorator(task_func)
        self.scheduler.add_job(
            decorated_task,
            'cron',
            id=f"{task_name}_{datetime.now().timestamp()}",
            name=task_name,
            priority=priority,
            **trigger_args
        )

    def start(self):
        """启动调度器"""
        self.scheduler.add_listener(self.scheduler_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self.scheduler.start()
        self.logger.info("调度器已启动，最大并发任务数: 3")

        try:
            while True:
                time.sleep(60)
                # 每小时执行健康检查
                if datetime.now().minute == 0:
                    self.log_health_status()
        except KeyboardInterrupt:
            self.scheduler.shutdown()
            self.logger.info("调度器已安全关闭")

    def log_health_status(self):
        """记录系统健康状态"""
        active_jobs = len(self.scheduler.get_jobs())
        running = len(self.running_tasks)
        self.logger.info(
            f"系统状态 | 活跃任务: {active_jobs} | 运行中任务: {running} | "
            f"最近错误: {self.scheduler._executors['default']._broken}"
        )

# 示例任务函数
def sample_task():
    print("执行示例任务...")
    time.sleep(10)

if __name__ == '__main__':
    manager = TaskManager()
    manager.add_task(sample_task, 'sample_task', hour='*')
    manager.start()
