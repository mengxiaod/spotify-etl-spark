import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from lambda_function import lambda_handler

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(result)
