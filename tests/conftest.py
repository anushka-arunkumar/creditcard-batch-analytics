import os, sys

# configure the test environment so tests can import project modules
# sys.path is a list of folders where Python looks when we import something
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

# sys.executable is the exact path of the Python executable currently running the code
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable