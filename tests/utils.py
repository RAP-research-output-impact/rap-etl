import os

# Directory where test data is stored.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__))
)

def read_file(path):
    with open(os.path.join(TEST_PATH, path)) as inf:
        return inf.read()