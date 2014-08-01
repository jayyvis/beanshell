
beanshell
=========
a python REPL shell to play with your beanstalkd server.

dependencies: PyYAML

### run it

```
beanshell
```

### Just do an ls

```
bean> ls

tube      	    ready	buried
------------------------------
default       	0	    0
email        	5	    0
image_process 	2	    1

3 tubes
```

### What more can you do with it ?

```
bean> help

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

```

### run a command directly from shell instead of getting in REPL mode

```
beanshell -c ls
```

### specify beanstalkd server address

By default, beanshell connects to ```localhost:11300``` . But you specify a different using -s or --server option.

```
beanshell -s 'jobserver:9888'
```

Or you can set it in an environment variable and just run ```beanshell```

```
export BEANSHELL_SERVER=jobserver:9888
```


