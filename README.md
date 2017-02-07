<snippet>
  <content>
# Parallel Task Planner

Language  - Python 3.4
Time Complexity - NP-Complete

Overview - This algorithm first tries to look at all possible permutations of every task and its dependency. If the number of nodes and its dependecy is higher then the alogorithm starts becoming slow. In that case it falls back into first possible solution by compromising accuracy. (Default hard-cut 2 minutes)

<p align="center">
        [![Parallel Task Planner explaination](https://img.youtube.com/vi/rKQaZuoUR4M/0.jpg)](https://img.youtube.com/vi/rKQaZuoUR4M/0.jpg?t=35s "Parallel Task Planner explaination")
</p>


## Installation
Entire code is in one file. I have used Python 3.4 interpreter. No need of external package or module.

## Usage
1. Clone git repository   git clone https://github.com/om471987/Task-Planner.git
2. Navigate to python file
3. Run the file using Python command line or install Pycharm
4. Please provide absolute/relative path for your test cases when the program starts.
5. I have added few test cases in test-cases directory

## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## Test Cases -
1. Given in the test
2. Two tasks ready to start parallely but with scare computes
3. Tasks with multiple
4. Error case - Task with more computation_required than available
5. Error case - Cycle detection

## Credits
Credit - 
1. Mission Peace - Detect Cycle in Directed Graph Algorithm
            https://www.youtube.com/watch?v=rKQaZuoUR4M
2. Assignment by Brian Bailey. (I looked at the readme document to understand the test but didn't look at the code.)
            https://github.com/baileyb/task-scheduler
</content>
  <tabTrigger>readme</tabTrigger>
</snippet>
# 
