# Task-Planner


Language  - Python
Time Complexity - NP-Complete

Overview - This algorithm first tries to look at all possible permutations of every task and its dependency. If the number of nodes and its dependecy is higher then the alogorithm starts becoming slow. In that case it falls back into first possible solution by compromising accuracy. (Default hard-cut 2 minutes)

Test Cases - 
1. Given in the test
2. Two tasks ready to start parallely but with scare computes
3. Tasks with multiple
4. Error case - Task with more computation_required than available
5. Error case - Cycle detection

Credit - 
1. Mission Peace - Detect Cycle in Directed Graph Algorithm
            https://www.youtube.com/watch?v=rKQaZuoUR4M
2. Assignment by Brian Bailey. (I looked at the readme document to understand the test but didn't look at the code.)
            https://github.com/baileyb/task-scheduler
