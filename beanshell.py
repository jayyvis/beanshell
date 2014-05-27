#!/usr/bin/python2.7
'''
REPL shell to interact with beanstalkd job queue

jayy 28-May-2014
'''

import beanstalkc as bean
import readline
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--host', default="localhost", help="beanstalkd hostname or ip address. defaults to localhost")
parser.add_argument('--port', default="4242", help="running port. defaults to 4242")
args = vars(parser.parse_args())

HOST = args['host']
PORT = int(args['port'])

queue = None

def connect():
    global queue
    queue = bean.Connection(HOST, PORT, parse_yaml=True)


class Commander(object):
    __instance = None
    
    @classmethod
    def eval(cls, cmd):
        if cls.__instance is None: cls.__instance = Commander()
        
        #parse command
        tokens = cmd.split(' ')
        cmd = tokens[0].replace('-', '_') #substitute underscore for hyphen
        args = tokens[1:] if len(tokens) > 1 else []
        
        #find the handler & invoke it
        try:
            handler = getattr(cls.__instance, cmd)
            return handler(*args)
        except AttributeError:
            return 'invalid command: %s. enter help or h to read the manual.' % cmd
        except Exception:
            return 'seems you got the syntax wrong. enter help or h to read the manual.'

    def __init__(self):
        self.put_validate_json_flag = True        
    
    def help(self):
        return """
    ls                       list tubes available in the queue
    
    stat <tube>              show statistics of queue.
                             optionally, specify <tube_name> to view a particular tube.
    
    inspect <tube>           prints all the ready jobs content of the specified tube
    
    clear <tube>             clears all the jobs of the specified tube
    
    pop <tube>               pops out a ready job from the specified tube & prints its content    
    
    pop-buried <tube>        pops out a buried job from the specified tube & prints its content    
    
    kick <tube>              kicks atmost bound job into ready queue
    
    put <tube> <job>         put / produce a job in to the tube
    
    ctrl+d                   quit the bean shell
"""
    def h(self):
        return self.help()
    
    def ls(self):
        tubes = queue.tubes()
        max_tubename_size = 1
        
        for tubename in tubes:
            if len(tubename) > max_tubename_size : max_tubename_size = len(tubename) 

        tubes_summary = '%s\t%s\t%s\n' % ('tube'.ljust(max_tubename_size), 'ready', 'buried')
        tubes_summary += '-' * 60 
                
        for tubename in tubes:
            stats = self.stat(tubename, parse=False)
            tubes_summary += '\n%s\t%d\t%d' % (tubename.ljust(max_tubename_size), stats['current-jobs-ready'], stats['current-jobs-buried'])

        tubes_summary += '\n\n%d tubes' % len(tubes)
                    
        return tubes_summary
    
    def stat(self, tubename=None, parse=True):
        stats = queue.stats_tube(tubename) if tubename else queue.stats()
        if not parse: return stats
        
        summary = '\n'.join(['%s : %s' % (k, v) for k, v in stats.iteritems()])
        header = 'stat for queue' if tubename is None else 'stat for tube : ' + tubename
        return header + '\n' + '-' * 20 + '\n' + summary

    def inspect(self, tubename):
        queue.watch(tubename)
    
        jobs = []
        stat = self.stat(tubename, parse=False)
        
        for i in xrange(stat['current-jobs-ready']):
            job = queue.reserve(timeout=5)
            if job is None: break
            
            jobs.append(job.body)
            job.release(delay=1)
        
        return '\n\n'.join(jobs) + '\n\n%d jobs' % len(jobs)
        
    def put(self, tubename, packet):
        if self.put_validate_json_flag:
            #validate packet is a json
            try:
                json.loads(packet)
            except:
                return 'ERROR: invalid json packet'
            
        queue.use(tubename)
        jid = queue.put(packet)
        return 'done:%s' % jid
    
    def put_validate_json(self):
        self.put_validate_json_flag = not self.put_validate_json_flag
        return self.put_validate_json_flag

    def pop(self, tubename):
        queue.watch(tubename)
    
        job = queue.reserve(timeout=5)
        
        if job:
            job.delete()
            return job.body
        else:
            return None

    def pop_buried(self, tubename):
        queue.use(tubename)
    
        job = queue.peek_buried()
        
        if job:
            job.delete()
            return job.body
        else:
            return None
        
    def kick(self, tubename):
        queue.use(tubename)
        return queue.kick()
    
    def clear(self, tubename):
        stat = self.stat(tubename, parse=False)
        
        #clear ready jobs
        ready_count = 0
        queue.watch(tubename)
        
        for i in xrange(stat['current-jobs-ready']):        
            job = queue.reserve(timeout=5)
            if job is None: break
            job.delete()
            ready_count += 1

        #clear buried jobs        
        buried_count = 0
        queue.use(tubename)
        
        for i in xrange(stat['current-jobs-buried']):
            job = queue.peek_buried()
            if job is None: break
            job.delete()
            buried_count += 1
                
        return 'cleared jobs. ready: %d  buried: %d' % (ready_count, buried_count)
        

#connect to beanstalk queue
connect()
print "connected to beanstalkd server running on %(host)s:%(port)s" % args

#begin the REPL
while True:
    #read
    try:
        cmd = str(raw_input('\nbean> '))
    except EOFError:
        print #a line break
        cmd = 'exit'
    
    #eval & print
    if cmd == 'exit': print 'bye'; break
    
    try:
        result = Commander.eval(cmd)
    except bean.SocketError:
        connect() #try connectining again
        result = Commander.eval(cmd)
        
    print result 
    
    #loop <-
    
    
