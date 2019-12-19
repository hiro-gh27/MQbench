import glob
import logging
import os
import subprocess
import sys
from datetime import datetime


def create_if_absent():
    go_src_path = "../../"
    os.chdir(go_src_path)
    if os.path.exists("bin/"):
        logging.info(print_ls())
    else:
        logging.info(print_ls())
        subprocess.check_call("mkdir bin/".split())


def print_ls():
    return subprocess.check_output("ls").decode("utf-8")


def print_bin_tree():
    print(subprocess.check_output("tree ./bin -L 3 -C".split()).decode("utf-8"))


def run_benchmark_tool(path: str, option: int):
    load = "-load=" + str(option)

    command = [path, load]
    print(command)
    subprocess.check_call(command)


def result_check(name: str) -> int:
    if os.path.exists("./results/" + name):
        return 5
    else:
        return 1


def read_result_then_decide(target_directory: str) -> int:
    target_directory = "./results/" + target_directory + "/*"
    files = glob.glob(target_directory)

    max_time = sys.float_info.min
    least_file = None

    for file in files:
        print(file)
        t = os.path.getmtime(file)
        dt = datetime.fromtimestamp(t)
        print(dt)
        if t > max_time:
            print("is call")
            least_file = str(file)
            max_time = t
    
    print("file:" + least_file + str(max_time))
    return 0


if __name__ == '__main__':
    create_if_absent()

    currentDirectory = os.getcwd()
    print(currentDirectory)

    binary_file_name = datetime.now().strftime("%Y%m%d_%H%M%S%f")

    r = subprocess.check_output("tree ./bin -L 3 -C".split())
    print(r.decode("utf-8"))

    if os.path.isfile(".bin/"):
        os.makedirs("./bin")

    binary_path = "bin/" + binary_file_name
    subprocess.check_call(["go", "build", "-o", binary_path])
    print_bin_tree()

    # execution for benchmark
    for i in range(3):
        load = result_check(binary_file_name)
        run_benchmark_tool(binary_path, load)
