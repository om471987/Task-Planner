<snippet>
  <content>
# Parallel Task Planner

 Language  - Python 3.4
 
 Time Complexity - NP-Complete

Overview - This algorithm first tries to look at all possible permutations of every task and its dependency. If the number of nodes and its dependecy is higher then the alogorithm starts becoming slow. In that case it falls back into first possible solution by compromising accuracy. (Default hard-cut 2 minutes)


## Installation
Entire code is in one file. I have used Python 3.4 interpreter. No need of external package or module.

## Usage
1. Clone git repository   (git clone https://github.com/om471987/Task-Planner.git)
2. Navigate at the root of 'Task-Planner' directory
3. Run the file using Python command line  (python3 Task-Planner/src/taskScheduler.py)
4. Please provide absolute/relative path for your test cases when the program starts.
5. I have added few test cases in test-cases directory

    ![image](https://github.com/om471987/Task-Planner/blob/master/content/Usage.png)

## Scenarios
  I try to create permutation of each task at each level for every computation. Thats the only guranteed way to find all possibilies and the minimum time.
  
1. Two Tasks starting at the same time
2. Two Tasks starting one after the other
3. One Task starts and second task starts before finishing the first.
4. One Task starts, second task starts before finishing the first seconds children start before first task ends.




## Future improvements
1. If multiple computes have same number of cores don't create task step for all of them. Remove redundancy.
2. Solve problem using Dynamic programming.

## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## Test Cases -
1. Given in the test
    ![image](https://github.com/om471987/Task-Planner/blob/master/content/given_in_test.jpg)

2. Two tasks ready to start parallely but with scare computes
  ![image](https://github.com/om471987/Task-Planner/blob/master/content/parallel_start_with_scarce_compute.jpg)
  
3. Complex Tree
  ![image](https://github.com/om471987/Task-Planner/blob/master/content/complex_task_tree.jpg)
    
4. Error case - Task with more computation_required than available

5. Error case - Cycle detection

## Credits
 1. Mission Peace - Detect Cycle in Directed Graph Algorithm
            https://www.youtube.com/watch?v=rKQaZuoUR4M
 2. Assignment by Brian Bailey. (I looked at the readme document to understand the test but didn't look at the code.)
            https://github.com/baileyb/task-scheduler
</content>
</snippet>
# 
