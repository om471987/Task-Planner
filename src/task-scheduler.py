import sys
import os
import heapq
import copy


class Task:
    def __init__(self, name, cores_required, execution_time):
        self.Name = name
        self.Cores_Required = cores_required
        self.Execution_Time = execution_time


class Compute:
    def __init__(self, name, capacity):
        self.Name = name
        self.Capacity = capacity


class Result:
    def __init__(self):
        self.Time = sys.maxsize
        self.Trace = ''
        self.Output = ''


class Step:
    def __init__(self, start_time, end_time, capacity, visited, output, trace):
        self.Capacity = capacity
        self.Start_Time = start_time
        self.End_Time = end_time
        self.Visited = visited
        self.Output = output
        self.Trace = trace + " End Time " + str(end_time)


def task_scheduler(tasks, computes):
    n = len(tasks)
    m = len(computes)

    visited = set()
    grey_set = set()
    result = Result()
    heap = []
    heapq.heapify(heap)
    task_range = dict()
    distinct_perm = []
    step_array = []
    template = [None for _ in range(m)]
    for i in range(m):
        template[i] = computes[i].Capacity

    i += 1
    for key, value in tasks.items():
        task_range[key] = i
        i += 1

    def clone_visited_set(input):
        output = dict()
        for k, v in input.items():
            output[k] = v
        return output

    def max_core_possibility():
        max_compute = 0
        for compute in computes:
            max_compute = max(max_compute, compute.Capacity)
        for task, parents in tasks.items():
            if task.Cores_Required > max_compute:
                return False
        return True

    def cycle_detection_util(task):
        visited.add(task)
        grey_set.add(task)
        for parent in tasks[task]:
            if parent in grey_set:
                return False
            if parent not in visited:
                if not cycle_detection_util(parent):
                    return False
        return True

    def cycle_detection():
        for task, parents in tasks.items():
            if task not in visited:
                grey_set.clear()
                if not cycle_detection_util(task):
                    return False
        return True

    def get_ready_for_execution_tasks(temp_visited):
        available = []
        for task in tasks:
            if task not in temp_visited:
                is_eligible = True
                for parent in tasks[task]:
                    if parent not in temp_visited:
                        is_eligible = False
                        break
                if is_eligible:
                    available.append(task)
        return available

    def get_tasks_with_no_parent():
        for task, parents in tasks.items():
            if len(parents) == 0:
                for j in range(m):
                    if task.Cores_Required <= computes[j].Capacity:
                        new_compute = copy.deepcopy(template)
                        new_compute[j] -= task.Cores_Required
                        new_visited = dict()
                        new_visited[task] = j
                        new_output = task.Name + ': ' + computes[j].Name
                        add_to_heap(0, task.Execution_Time, new_compute, new_visited, new_output, 'Initial')

    def log_heap(event, end_time, new_capacity, new_visited, output):
        visited = ''
        for key in new_visited.keys():
            visited += key.Name + ', '
        visited += ' $'
        # print(event, end_time, new_capacity, visited, output)

    def get_unique_keys(new_visited):
        temp = ''
        for key in new_visited.keys():
            temp += str(task_range[key])
        temp2 = sorted(temp)
        output = ''
        for k in temp2:
            output += k
        return output

    def add_to_heap(new_start_time, new_end_time, new_capacity, new_visited, new_output, trace):
        unique_keys = get_unique_keys(new_visited)
        if (new_end_time, new_capacity, unique_keys) not in distinct_perm and new_end_time < result.Time:
            step = Step(new_start_time, new_end_time, new_capacity, new_visited, new_output, trace)
            step_counter = len(step_array)
            step_array.append(step)
            heapq.heappush(heap, (new_end_time, step_counter))
            log_heap('added', new_end_time, new_capacity, new_visited, new_output)
            distinct_perm.append((new_end_time, new_capacity, unique_keys))

    def permutation_concurrent_start(start_t, old_step, ready_tasks):
        for task in ready_tasks:
            if task in old_step.Visited:
                continue
            # log_heap('try perm for ' + task.Name, old_step.End_Time, old_step.Capacity, old_step.Visited, old_step.Output)
            parents = tasks[task]
            is_eligibile = True
            for parent in parents:
                if parent in old_step.Visited:
                    is_eligibile = False
                    break
            if is_eligibile:
                for o in range(m):
                    new_capacity = copy.deepcopy(old_step.Capacity)
                    if new_capacity[o] >= task.Cores_Required:
                        new_visited = clone_visited_set(old_step.Visited)
                        new_visited[task] = o
                        new_capacity[o] -= task.Cores_Required
                        new_end_time = max(old_step.End_Time, start_t + task.Execution_Time)
                        new_output = old_step.Output + ' * ' + task.Name + ': ' + computes[o].Name
                        add_to_heap(start_t, new_end_time, new_capacity, new_visited, new_output, old_step.Trace + ' | start Time ' + str(start_t))

    def permutation_sequential_start(old_step, ready_tasks):
        for task in ready_tasks:
            for o in range(m):
                if task.Cores_Required <= old_step.Capacity[o]:
                    new_capacity = copy.deepcopy(old_step.Capacity)
                    new_capacity[o] -= task.Cores_Required
                    new_visited = clone_visited_set(old_step.Visited)
                    new_visited[task] = o
                    new_output = old_step.Output + ' * ' + task.Name + ': ' + computes[o].Name
                    add_to_heap(old_step.End_Time, old_step.End_Time + task.Execution_Time, new_capacity, new_visited,
                                new_output, old_step.Trace + ' | start Time ' + str(old_step.End_Time))

    def permutation_non_concurrent_start(start_t, ready_tasks):
        for task in ready_tasks:
            for old_end_time, old_step_counter in heap:
                old_step = step_array[old_step_counter]
                # log_heap('try perm for ' + task.Name, old_end_time, old_step.Capacity, old_step.Visited, old_step.Output)
                parents = tasks[task]
                is_eligibile = True
                if task in old_step.Visited:
                    continue
                for parent in parents:
                    if parent not in old_step.Visited:
                        is_eligibile = False
                        break
                if is_eligibile:
                    for o in range(m):
                        new_capacity = copy.deepcopy(old_step.Capacity)
                        new_visited = clone_visited_set(old_step.Visited)
                        new_visited[task] = o
                        new_end_time = max(old_end_time, start_t + task.Execution_Time)
                        new_output = old_step.Output + ' * ' + task.Name + ': ' + computes[o].Name
                        add_to_heap(start_t, new_end_time, new_capacity, new_visited, new_output, old_step.Trace + ' | start Time ' + str(start_t))

    def get_minimum_exection_time(step):
        # print(step.Output, step.Trace)
        if step.End_Time < result.Time:
            result.Time = step.End_Time
            result.Output = step.Output
            result.Trace = step.Trace

    def execute():
        get_tasks_with_no_parent()
        while len(heap) > 0:
            end_time, step_counter = heapq.heappop(heap)
            step = step_array[step_counter]
            log_heap('Removed', end_time, step.Capacity, step.Visited, step.Output)
            if end_time >= result.Time:
                continue
            if len(step.Visited) == n:
                get_minimum_exection_time(step)
                continue
            ready_tasks = get_ready_for_execution_tasks(step.Visited)
            permutation_concurrent_start(step.Start_Time, step, ready_tasks)
            for k, v in step.Visited.items():
                step.Capacity[v] += k.Cores_Required
            permutation_sequential_start(step, ready_tasks)
            permutation_non_concurrent_start(end_time, ready_tasks)

    if not max_core_possibility():
        print('Max core not possible')
    elif not cycle_detection():
        print('Cycle found')
    else:
        execute()
        print('Min Time:' + str(result.Time))
        print(result.Output.replace('* ', '\n'))
        print(result.Trace)


def get_tasks(path):
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
                        print('parent_tasks has a value ' + parent_name.strip() + ' which was not found before')
                        return None
                    parents.append(task_names[parent_name.strip()])
            task = Task(name, core, time)
            task_names[name] = task
            tasks[task] = parents
            found_task, found_core, found_time, name, core, time, parents = False, False, False, '', 0, 0, []
    return tasks


def get_computes(path):
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


def test_case(task_path, compute_path):
    path = os.path.join(os.path.dirname(__file__), task_path)
    tasks = get_tasks(path)
    if not tasks:
        print('Please fix tasks file')
        return
    path = os.path.join(os.path.dirname(__file__), compute_path)
    computes = get_computes(path)
    if not computes:
        print('Please fix computes file')
        return
    task_scheduler(tasks, computes)


#test_case('../test-cases/testcase1/tasks.yaml', '../test-cases/testcase1/computes.yaml') # 210
test_case('../test-cases/testcase2/tasks.yaml', '../test-cases/testcase2/computes.yaml') # 350
#test_case('../test-cases/testcase3/tasks.yaml', '../test-cases/testcase3/computes.yaml') # 350
# test_case(input(), input())