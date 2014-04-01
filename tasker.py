from config import *
from twisted.web import server, resource
from twisted.web.resource import Resource
from twisted.internet import reactor, threads
from twisted.python import threadpool
from twisted.internet.task import LoopingCall
import os, re, urllib, urllib2, subprocess

class Task:

    TYPE = 'TASK'

    def __init__(self, url = ''):        
        self.url = url

    def execute(self):
        pass

    def __str__(self):
        return self.url


class ShellTask(Task):

    TYPE = "SHELL"

    def execute(self):
        cmd = (SCRIPT_PATH % self.url).split(" ")
        print "   Call: %s\n" % cmd
        subprocess.call(cmd)


class UrlTask(Task):

    TYPE = "URL"

    def execute(self):
        url = API_BASE_URL + self.url
        print "   Request: %s\n" % url
        print urllib2.urlopen(url).read()


class Scheduler(resource.Resource):

    CALL_SHELL = "shell"
    CALL_URL = "url"
    CALL_STOP = "stop"

    def __init__(self):
        Resource.__init__(self)
        self.putChild('', self)
        self.threadpool = threadpool.ThreadPool(MIN_THREADS, MAX_THREADS)
        self.threadpool.start()
        self._queue_regular = []
        self._is_queue_loaded = False
        self._load_queue_timer = LoopingCall(self._load_queue)
        self._load_queue_timer.start(LOAD_QUEUE_DELAY, False)
        self.types = [ShellTask, UrlTask]

    def _terminate(self):
        self.threadpool.stop()
        reactor.stop()

    def render_GET(self, request):        
        print("Scheduler.render_GET(): %s" % request.args)

        if request.args.get(self.CALL_STOP) != None:
            self._terminate()
            return self._formatMsg("Terminate")

        if self.CALL_SHELL in request.args:
            task = ShellTask(request.args.get(self.CALL_SHELL)[0])
        elif self.CALL_URL in request.args:
            task = UrlTask(request.args.get(self.CALL_URL)[0])
        else:
            return self._formatMsg("  Action not found")

        if self._is_task_unique(task):
            self._queue_regular.append(task) 
            self._save_queue() 
            self._run_task(task)

            return self._formatMsg("  Task added")
        else:        
            return self._formatMsg("  Task already exists")

    def _formatMsg(self, msg):
        print(msg)
        return "<html>%s</html>" % msg

    def _is_task_unique(self, task):
        unique = True
        for queue_task in self._queue_regular:
            print str(task)
            if queue_task.url.strip() == str(task.url):
                unique = False
                break
        return unique

    def _remove_task(self, task):               
        print("  Scheduler._remove_queue(): Remove task: %s" % task.url)
        self._queue_regular.remove(task)
        self._save_queue()

    def _save_queue(self):                
        if self._is_queue_loaded == True:
            print("  Scheduler._save_queue")
            with open(FILE_QUEUE_REGULAR, "wb") as f:
                for task in self._queue_regular:
                    print str(task)
                    f.write("%s|%s\n" % (task.TYPE, str(task)))
                f.close()

    def _load_queue(self):
        if self._load_queue_timer is not None:
            self._load_queue_timer.stop()
        if os.path.isfile(FILE_QUEUE_REGULAR):
            with open(FILE_QUEUE_REGULAR, "r") as f:
                lines = f.readlines()
                print("Scheduler._load_queue(): Loaded count regular tasks: %s" % len(lines))
                for line in lines:
                    if line.strip() == "":
                        continue
                    line_list = line.strip().split('|')

                    task = None

                    for type in self.types:
                        print("%s == %s\n" % (line_list[0], type.TYPE))
                        if line_list[0] == type.TYPE:
                            task = type(line_list[1])

                    if task == None:
                        print("Cant parse task: %s \n" % line)
                        continue                    

                    if self._is_task_unique(task):
                         print("Scheduler._load_queue(): Save task: %s" % task)
                         self._queue_regular.append(task)
                         self._run_task(task)

                f.close()
        self._is_queue_loaded = True        

    def _run_task(self, task):
        print(" Scheduler._run_task(): Run task: %s " % task.url)
        d = threads.deferToThreadPool(reactor, self.threadpool, self._do_task, task)
        d.addErrback(self._error_handler)

    def _do_task(self, task):
        print("  Scheduler._do_task(): Do task: %s " % task.url)
        try:            
            task.execute()
        except Exception as error:
            print error
        finally:
            self._remove_task(task)
        print(" Finished task: %s " % task.url)

    def _error_handler(self, result):
        print(" Error happened ")
        print(result)


factory = server.Site(Scheduler())
reactor.listenTCP(PORT, factory)
reactor.run()