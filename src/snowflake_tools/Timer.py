import time

class Timer:
    def __init__(self, output=False):
        self.output = output

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time

        if self.output:
            print(f"Time elapsed: {elapsed_time:.4f} seconds")
