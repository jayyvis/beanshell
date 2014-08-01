#!/usr/bin/python2.7
'''
REPL shell to interact with beanstalkd job queue
'''

from lib import beanstalkc
import readline
import json
import argparse
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', default=os.getenv('BEANSHELL_SERVER', 'localhost:11300'), help='beanstalkd server address. defaults to localhost:11300')
    parser.add_argument('-c', '--cmd', default=None, help='run a command directly in shell instead of REPL')
    
    args = vars(parser.parse_args())
    
    SERVER = args['server']
    
    HOST, PORT = SERVER.split(':') 

    #connect to beanstalk queue
    queue = beanstalkc.Connection(HOST, int(PORT), parse_yaml=True)
    commander = Commander(queue)

    #=======================================================================
    # command line mode
    #=======================================================================
    if args['cmd']:
        result = commander.eval(args['cmd'])
        print result
        return
    
    #===========================================================================
    # REPL mode
    #===========================================================================
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
        
        result = commander.eval(cmd)
        print result

        #loop <-


class Commander(object):
    def __init__(self, queue):
        self._queue = queue
        self._validate_json = True        

    def eval(self, cmd):
        #parse command
        tokens = cmd.split(' ')
        cmd = tokens[0].replace('-', '_') #substitute underscore for hyphen
        args = tokens[1:] if len(tokens) > 1 else []
        
        #find the handler & invoke it
        try:
            handler = getattr(self, cmd)
            return handler(*args)
        except AttributeError:
            return 'invalid command: %s. enter help or h to read the manual.' % cmd
        except Exception:
            return 'seems you got the syntax wrong. enter help or h to read the manual.'

    
    def help(self):
        return """
    ls                       list tubes available in the queue

    stat [<tube>]            show statistics of queue.
                             optionally, specify <tube> name to view a particular tube.
    
    inspect <tube>           prints all the ready jobs content of the specified tube
    
    clear <tube>             clears all the jobs of the specified tube
    
    pop <tube>               pops out a ready job from the specified tube & prints its content    
    
    pop-buried <tube>        pops out a buried job from the specified tube & prints its content    
    
    kick <tube>              kicks atmost bound job into ready queue
    
    put <tube> <job>         put / produce a job in to the tube
    
    json                     turn ON/OFF json validation while putting a job
    
    ctrl+d                   quit the bean shell
"""

    def h(self):
        return self.help()
    
    def json(self):
        self._validate_json = not self._validate_json
        return 'json validation ' + ('ON' if self._validate_json else 'OFF')
    
    def ls(self):
        tubes = self._queue.tubes()
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
        stats = self._queue.stats_tube(tubename) if tubename else self._queue.stats()
        if not parse: return stats
        
        summary = '\n'.join(['%s : %s' % (k, v) for k, v in stats.iteritems()])
        header = 'stat for queue' if tubename is None else 'stat for tube : ' + tubename
        return header + '\n' + '-' * 20 + '\n' + summary

    def inspect(self, tubename):
        self._queue.watch(tubename)
    
        jobs = []
        stat = self.stat(tubename, parse=False)
        
        for i in xrange(stat['current-jobs-ready']):
            job = self._queue.reserve(timeout=5)
            if job is None: break
            
            jobs.append(job.body)
            job.release(delay=1)
        
        return '\n\n'.join(jobs) + '\n\n%d jobs' % len(jobs)
        
    def put(self, tubename, packet):
        if self._validate_json:
            #validate packet is a json
            try:
                json.loads(packet)
            except:
                return 'ERROR: invalid json packet'
            
        self._queue.use(tubename)
        jid = self._queue.put(packet)
        return 'done:%s' % jid

    def pop(self, tubename):
        self._queue.watch(tubename)
    
        job = self._queue.reserve(timeout=5)
        
        if job:
            job.delete()
            return job.body
        else:
            return None

    def pop_buried(self, tubename):
        self._queue.use(tubename)
    
        job = self._queue.peek_buried()
        
        if job:
            job.delete()
            return job.body
        else:
            return None
        
    def kick(self, tubename):
        self._queue.use(tubename)
        return self._queue.kick()
    
    def clear(self, tubename):
        stat = self.stat(tubename, parse=False)
        
        #clear ready jobs
        ready_count = 0
        self._queue.watch(tubename)
        
        for i in xrange(stat['current-jobs-ready']):        
            job = self._queue.reserve(timeout=5)
            if job is None: break
            job.delete()
            ready_count += 1

        #clear buried jobs        
        buried_count = 0
        self._queue.use(tubename)
        
        for i in xrange(stat['current-jobs-buried']):
            job = self._queue.peek_buried()
            if job is None: break
            job.delete()
            buried_count += 1
                
        return 'cleared jobs. ready: %d  buried: %d' % (ready_count, buried_count)
    

if __name__ == '__main__':
    main()
    