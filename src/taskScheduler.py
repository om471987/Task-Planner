#!/usr/bin/python
__author__ = "Omkar Panhalkar"

import sys
import heapq
import copy
import time
import os
import collections


class Task(object):
    def __init__(self, name, cores_required, execution_time):
        self.Name = name
        self.Cores_Required = cores_required
        self.Execution_Time = execution_time


class Compute(object):
    def __init__(self, name, capacity):
        self.Name = name
        self.Capacity = capacity


class Result(object):
    def __init__(self):
        self.Time = sys.maxsize
        self.Output = ''
        self.Timer_Start = time.time()
        self.Max_Waiting_Time = 120# Seconds
        self.TraceLevel = 1

    def is_max_waiting_time_reached(self):
        return int(time.time() - self.Timer_Start) >= self.Timer_Start


class Step(object):
    def __init__(self, start_time, end_time, capacity, visited, output, black_Set, task_Start_Times):
        self.Capacity = capacity
        self.Start_Time = start_time
        self.Task_Start_Times = task_Start_Times
        self.End_Time = end_time
        self.Black_Set = black_Set
        self.Visited = visited
        self.Output = output


class dataImporter(object):

    def get_tasks(self, path):
        with open(path) as f:
            content = f.readlines()
        tasks = dict()
        task_names = dict()
        i = 0
        n = len(content)
        found_task, found_core, found_time, name, core, time, parents = False, False, False, '', 0, 0, []
        while i < n:
            current = content[i].strip()
            if len(current) == 0:
                i += 1
                continue
            elif ':' not in current or current.count(':') != 1:
                print('Did not include line "' + current + '"from tasks file')
                return None

            before, after = current.split(':')
            if not found_task:
                found_task = True
                name = before
                i += 1
                continue
            if found_task and not found_core:
                found_core = True
                if before.strip() != 'cores_required':
                    print('cores_required not found on line "' + current + '"from tasks file')
                    return None
                elif not after.strip().isnumeric():
                    print('cores_required does not have integer value on line "' + current + '"from tasks file')
                    return None
                core = int(after)
                i += 1
                continue
            if found_task and found_core and not found_time:
                found_time = True
                if before.strip() != 'execution_time':
                    print('execution_time not found on line "' + current + '"from tasks file')
                    return None
                elif not after.strip().isnumeric():
                    print('execution_time does not have integer value on line "' + current + '"from tasks file')
                    return None
                time = int(after)
                i += 1
                continue
            if found_task and found_core and found_time:
                if before.strip() == 'parent_tasks':
                    i += 1
                    parent_names = after.strip().replace('"', '').split(',')
                    for parent_name in parent_names:
                        if parent_name.strip() not in task_names:
                            print('(Potential cycle) parent_tasks has a value ' + parent_name.strip() + ' which was not found before')
                            return None
                        parents.append(task_names[parent_name.strip()])
                task = Task(name, core, time)
                task_names[name] = task
                tasks[task] = parents
                found_task, found_core, found_time, name, core, time, parents = False, False, False, '', 0, 0, []
        return tasks

    def get_computes(self, path):
        with open(path) as f:
            content = f.readlines()
        computes = []
        for line in content:
            current = line.strip()
            if len(current) == 0:
                continue
            elif ':' not in current or current.count(':') != 1:
                print('Did not include line "' + current + '"from computes file')
                return None
            name, capacity = current.split(':')
            if capacity.strip().isnumeric():
                computes.append(Compute(name, int(capacity.strip())))
        return computes

    def validate_inputs(self, tasks, computes):
        max_compute = 0
        for compute in computes:
            if compute.Capacity < 0:
                print('Compute capacity for ' + compute.Name + 'should be a positive value')
                return False
            max_compute = max(max_compute, compute.Capacity)
        for task, parents in tasks.items():
            if task.Cores_Required < 0:
                print('Cores_Required for ' + task.Name + 'should be a positive value')
                return False
            if task.Cores_Required > max_compute:
                print('Cores_Required for ' + task.Name + ' can not be more than given compute capacity')
                return False
        return True

    def get_data(self):
        print('Please enter absolute/relative path for tasks file: \n')
        task_file_name = input()
        print('Please enter absolute/relative path for computes file: \n')
        computes_file_name = input()

        path = os.path.join(os.path.dirname(__file__), task_file_name)
        tasks = self.get_tasks(path)
        if not tasks:
            print('Please fix tasks file')
            return
        path = os.path.join(os.path.dirname(__file__), computes_file_name)
        computes = self.get_computes(path)
        if not computes:
            print('Please fix computes file')
            return
        if self.validate_inputs(tasks, computes):
            return tasks, computes
        else:
            return None, None


class taskScheduler(object):
    def __init__(self):
        self.result = None
        self.n = None
        self.m = None
        self.tasks = None
        self.computes = None
        self.heap = None
        self.task_range = None
        self.distinct_perm = None
        self.step_array = None
        self.template = None
        self.dependency = None

    def initialize(self):
        self.result = Result()
        self.n = len(self.tasks)
        self.m = len(self.computes)

        self.heap = []
        self.task_range = dict()
        self.distinct_perm = []
        self.step_array = []
        self.template = [None for _ in range(self.m)]
        self.dependency = collections.deque()
        # generate unique key for a task
        counter = 1
        for key, value in self.tasks.items():
            self.task_range[key] = counter
            counter += 1
        # generate template to keep track of maximum compute
        for counter in range(self.m):
            self.template[counter] = self.computes[counter].Capacity

    def cycle_detection(self, tasks):
        black_set = set()
        grey_set = set()

        def cycle_detection_util(task):
            black_set.add(task)
            grey_set.add(task)
            for parent in tasks[task]:
                if parent in grey_set:
                    return False
                if parent not in black_set:
                    if not cycle_detection_util(parent):
                        return False
            self.dependency.append(task)
            return True

        for task, parents in tasks.items():
            if task not in black_set:
                grey_set.clear()
                if not cycle_detection_util(task):
                    print('Not a valid DAG. Please remove the Cycle')
                    return False
        return True

    def clone_visited_set(self, visited):
        output = dict()
        for key, value in visited.items():
            output[key] = value
        return output

    def get_ready_for_execution_tasks_with_children(self, visited):
        output = []
        for task in self.dependency:
            if task not in visited:
                is_eligible = True
                for parent in self.tasks[task]:
                    if parent not in visited:
                        is_eligible = False
                        break
                if is_eligible:
                    output.append(task)
        return output

    def get_ready_for_execution_tasks_without_children(self, visited, black_set):
        output = []
        traversed = set()
        for task in self.dependency:
            if task not in visited:
                parents = self.tasks[task]
                is_eligible = True
                for parent in parents:
                    if (parent in visited or parent in traversed) and parent not in black_set:
                        is_eligible = False
                        break
                if is_eligible:
                    output.append(task)
                traversed.add(task)
        return output

    def get_tasks_with_no_parent(self):
        for task, parents in self.tasks.items():
            if len(parents) == 0:
                for compute_counter in range(self.m):
                    if task.Cores_Required <= self.computes[compute_counter].Capacity:
                        new_compute = copy.deepcopy(self.template)
                        new_compute[compute_counter] -= task.Cores_Required
                        new_visited = dict()
                        new_visited[task] = compute_counter
                        new_output = task.Name + ': ' + self.computes[compute_counter].Name
                        task_Start_Times = dict()
                        task_Start_Times[task] = 0
                        self.add_to_heap(0, task.Execution_Time, new_compute, new_visited, new_output, dict(), task_Start_Times)

    def log_heap(self, event, logging_step):
        string_builder = ' ### '
        for key in logging_step.Visited.keys():
            string_builder += key.Name + ', '
        if self.result.TraceLevel > 2:
            print(event, logging_step.Start_Time, logging_step.Capacity, logging_step.End_Time, logging_step.Output, string_builder)

    '''
    This will generate unique hash for similar group of steps, which help narrow down futher steps
    (This optimization does not affect accuracy of the final result)
    '''
    def get_unique_keys(self, new_visited):
        temp = ''
        for key in new_visited.keys():
            temp += str(self.task_range[key])
        temp2 = sorted(temp)
        output = ''
        for k in temp2:
            output += k
        return output

    def add_to_heap(self, new_start_time, new_end_time, new_capacity, new_visited, new_output, black_set, task_Start_Times):
        new_capacity = copy.deepcopy(new_capacity)
        new_visited = self.clone_visited_set(new_visited)
        black_set = self.clone_visited_set(black_set)
        task_Start_Times = self.clone_visited_set(task_Start_Times)
        unique_keys = self.get_unique_keys(new_visited)
        if (new_end_time, new_capacity, unique_keys) not in self.distinct_perm and new_end_time < self.result.Time:
            added_step = Step(new_start_time, new_end_time, new_capacity, new_visited, new_output, black_set, task_Start_Times)
            step_counter = len(self.step_array)
            self.step_array.append(added_step)
            heapq.heappush(self.heap, (new_end_time, step_counter))
            self.log_heap('Added', added_step)
            self.distinct_perm.append((new_end_time, new_capacity, unique_keys))
            return added_step
        else:
            return None

    def multiple_tasks_that_start_parallel(self, popped_step):
        ready_tasks = self.get_ready_for_execution_tasks_without_children(popped_step.Visited, popped_step.Black_Set)
        start_time = popped_step.Start_Time
        end_time = popped_step.End_Time
        capacity = copy.deepcopy(popped_step.Capacity)
        visited = self.clone_visited_set(popped_step.Visited)
        black_set = self.clone_visited_set(popped_step.Black_Set)
        task_Start_Times = self.clone_visited_set(popped_step.Task_Start_Times)
        output = popped_step.Output
        while len(ready_tasks) > 0:
            for ready_task in ready_tasks:
                for compute_counter in range(self.m):
                    if capacity[compute_counter] >= ready_task.Cores_Required and ready_task not in visited:
                        end_time = max(end_time, start_time + ready_task.Execution_Time)
                        capacity[compute_counter] -= ready_task.Cores_Required
                        visited[ready_task] = compute_counter
                        output += " * " + ready_task.Name + ': ' + self.computes[compute_counter].Name
                        task_Start_Times[ready_task] = start_time
                        self.add_to_heap(start_time, end_time, capacity, visited, output, black_set, task_Start_Times)
            if len(visited) == self.n:
                self.get_minimum_execution_time(end_time, output)
            ready_tasks = self.get_ready_for_execution_tasks_without_children(visited, black_set)
            if len(ready_tasks) == 0:
                min_start_time = sys.maxsize
                min_start_task = None
                for task_for_completion in visited:
                    if task_for_completion not in black_set and (start_time + task_for_completion.Execution_Time) < min_start_time:
                        min_start_time = task_Start_Times[task_for_completion] + task_for_completion.Execution_Time
                        min_start_task = task_for_completion
                capacity[visited[min_start_task]] += min_start_task.Cores_Required
                black_set[min_start_task] = None
                start_time = min_start_time
                ready_tasks = self.get_ready_for_execution_tasks_without_children(visited, black_set)
            else:
                break

    def single_task_that_start_parallel(self, popped_step):
        ready_tasks = self.get_ready_for_execution_tasks_without_children(popped_step.Visited, popped_step.Black_Set)
        for ready_task in ready_tasks:
            for compute_counter in range(self.m):
                if popped_step.Capacity[compute_counter] >= ready_task.Cores_Required:
                    capacity = copy.deepcopy(popped_step.Capacity)
                    visited = self.clone_visited_set(popped_step.Visited)
                    visited[ready_task] = compute_counter
                    end_time = max(popped_step.End_Time, popped_step.Start_Time + ready_task.Execution_Time)
                    capacity[compute_counter] -= ready_task.Cores_Required
                    output = popped_step.Output + " * " + ready_task.Name + ': ' + self.computes[compute_counter].Name
                    task_Start_Times = self.clone_visited_set(popped_step.Task_Start_Times)
                    task_Start_Times[ready_task] = popped_step.Start_Time
                    created_step = self.add_to_heap(popped_step.Start_Time, end_time, capacity, visited, output, popped_step.Black_Set, task_Start_Times)
                    if created_step:
                        self.multiple_tasks_that_start_parallel(created_step)

    def tasks_that_start_after_completion(self, popped_step):
        ready_tasks = self.get_ready_for_execution_tasks_with_children(popped_step.Visited)
        for ready_task in ready_tasks:
            for compute_counter in range(self.m):
                if popped_step.Capacity[compute_counter] >= ready_task.Cores_Required:
                    capacity = copy.deepcopy(popped_step.Capacity)
                    visited = self.clone_visited_set(popped_step.Visited)
                    visited[ready_task] = compute_counter
                    end_time = popped_step.End_Time + ready_task.Execution_Time
                    capacity[compute_counter] -= ready_task.Cores_Required
                    output = popped_step.Output + " * " + ready_task.Name + ': ' + self.computes[compute_counter].Name
                    task_Start_Times = self.clone_visited_set(popped_step.Task_Start_Times)
                    task_Start_Times[ready_task] = popped_step.End_Time
                    created_step = self.add_to_heap(popped_step.End_Time, end_time, capacity, visited, output, popped_step.Black_Set, task_Start_Times)
                    if created_step:
                        self.multiple_tasks_that_start_parallel(created_step)

    def get_minimum_execution_time(self, end_time, output):
        if end_time < self.result.Time:
            if self.result.TraceLevel > 1:
                print('Min value found', end_time, output)
            self.result.Time = end_time
            self.result.Output = output

    def execute(self):
        self.get_tasks_with_no_parent()
        while len(self.heap) > 0:
            end_time, step_counter = heapq.heappop(self.heap)
            if end_time >= self.result.Time:
                continue
            popped_step = self.step_array[step_counter]
            self.log_heap('Removed', popped_step)
            if len(popped_step.Visited) == self.n:
                self.get_minimum_execution_time(popped_step.End_Time, popped_step.Output)
                if self.result.is_max_waiting_time_reached():
                    return
            self.multiple_tasks_that_start_parallel(popped_step)
            self.single_task_that_start_parallel(popped_step)
            for visited_task, compute_counter in popped_step.Visited.items():
                if visited_task not in popped_step.Black_Set:
                    popped_step.Capacity[compute_counter] += visited_task.Cores_Required
                    popped_step.Black_Set[visited_task] = None
            self.tasks_that_start_after_completion(popped_step)

    def run(self):
        aass = dataImporter()
        tasks, computes = aass.get_data()
        if not tasks or not computes:
            return
        self.tasks = tasks
        self.computes = computes
        self.initialize()

        if not self.cycle_detection(self.tasks):
            return
        self.execute()
        print('Min Time:' + str(self.result.Time))
        print(self.result.Output.replace('* ', '\n'))
        print('Time elapsed in seconds:', round(time.time() - self.result.Timer_Start, 2))

if __name__ == '__main__':
    taskScheduler = taskScheduler()
    taskScheduler.run()

# test_case('../test-cases/complex_task_tree/tasks.yaml', '../test-cases/complex_task_tree/computes.yaml') # Answer - 210
# test_case('../test-cases/given_in_test/tasks.yaml', '../test-cases/given_in_test/computes.yaml')  # Answer - 350
# test_case('../test-cases/parallel_start_with_scarce_compute/tasks.yaml', '../test-cases/parallel_start_with_scarce_compute/computes.yaml')  # Answer - 360
# test_case('../test-cases/error_cases/cycle_found/tasks.yaml', '../test-cases/error_cases/cycle_found/computes.yaml') # Error
# test_case('../test-cases/error_cases/core_requires_exceed/tasks.yaml', '../test-cases/error_cases/core_requires_exceed/computes.yaml') # Error
