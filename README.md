NOTE: code is yet to be published. we are refactoring it out of our management tools. raise a ticket to indicate us to prioritize.

beanshell
=========
a python REPL shell to play with your beanstalkd server.


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
    
    stat <tube>              show statistics of queue.
                             optionally, specify <tube_name> to view a particular tube.
    
    inspect <tube>           prints all the ready jobs content of the specified tube
    
    clear <tube>             clears all the jobs of the specified tube
    
    pop <tube>               pops out a ready job from the specified tube & prints its content    
    
    pop-buried <tube>        pops out a buried job from the specified tube & prints its content    
    
    put <tube> <job>         put / produce a job in to the tube
    
    ctrl+d                   quit the bean shell

```

