import os
import threading
import queue
import time
from collections import defaultdict

TIMEOUT_THRESHOLD = 5 
MAX_THREADS = 1020     

task_queue = queue.Queue()

# Track file processing status
task_status = {}
task_lock = threading.Lock()

# Global list to store local processing results
global_results = []
result_lock = threading.Lock()

def process_file(filename):
    local_counts = defaultdict(int)
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                chains = content.split(", ")
                for chain in chains:
                    if chain:
                        local_counts[chain] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return local_counts

def worker():
    # Worker function to process files from the queue
    while True:
        try:
            filename = task_queue.get(timeout=1)
        except queue.Empty:
            break  # No more files to process

        with task_lock:
            task_status[filename] = ("in_progress", time.time())

        local_result = process_file(filename)

        with task_lock:
            task_status[filename] = ("done", time.time())
        with result_lock:
            global_results.append(local_result)

        task_queue.task_done()


def monitor():
    # Monitor function to reassign files that timeout
    while True:
        time.sleep(1)
        with task_lock:
            for filename, (status, start_time) in list(task_status.items()):
                if status == "in_progress" and (time.time() - start_time) > TIMEOUT_THRESHOLD:
                    print(f"Monitor: Reassigning file {filename} due to timeout.")
                    task_status[filename] = ("pending", None)
                    task_queue.put(filename)
        # Exit monitor if queue is empty and all files are done
        if task_queue.empty():
            with task_lock:
                all_done = all(status == "done" for status, _ in task_status.values())
            if all_done:
                break


def merge_results(results):
    # Merge all hash tables produced by the threads into one dictionary
    merged = defaultdict(int)
    for res in results:
        for chain, count in res.items():
            merged[chain] += count
    return merged


def main():
    data_dir = "data"
    if not os.path.isdir(data_dir):
        print(f"Error: The data directory '{data_dir}' does not exist.")
        return

    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)
             if os.path.isfile(os.path.join(data_dir, f))]

    with task_lock:
        for file in files:
            task_status[file] = ("pending", None)
            task_queue.put(file)

    num_workers = min(MAX_THREADS, len(files))
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()

    task_queue.join()

    for t in threads:
        t.join()
    monitor_thread.join()

    merged_results = merge_results(global_results)
    if not merged_results:
        print("No data found.")
        return

    max_chain = None
    max_count = -1
    for chain, count in merged_results.items():
        if count > max_count:
            max_count = count
            max_chain = chain

    print(f"\nWinner: {max_chain} with {max_count} hamburgers sold.")


if __name__ == "__main__":
    main()
