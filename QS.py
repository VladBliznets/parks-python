from Pyro4 import expose
import heapq

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Initialized")

    def solve(self):
        print("Job Started")
        k = len(self.workers)
        print("Workers %d" % k)
        arr = self.read_input()
        n = len(arr)

        mapped = []
        for i in range(k):
            print("map %d" % i)
            mapped.append(self.workers[i].mymap(arr[(n * i // k): (n * (i + 1) // k)]))
        result = self.myreduce(mapped)
        self.write_output(result)
        print("Job Finished")

    @staticmethod
    @expose
    def mymap(arr):
        if len(arr) <= 1:
            return arr
        return Solver.quick_sort(arr)

    @staticmethod
    @expose
    def myreduce(mapped):
        return list(heapq.merge(part.value for part in mapped))

    @staticmethod
    def quick_sort(arr):
        if len(arr) <= 1:
            return arr
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        return Solver.quick_sort(left) + middle + Solver.quick_sort(right)

    def read_input(self):
        with open(self.input_file_name, 'r') as f:
            return list(map(int, f.readlines()))

    def write_output(self, output):
        with open(self.output_file_name, 'w') as f:
            f.writelines([str(item) + '\n' for item in output])
        print("output done")
