import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_sdk_path = os.path.join(_repo_root, "libs", "sdk", "python")
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
if _sdk_path not in sys.path:
    sys.path.insert(0, _sdk_path)

# Re-export tests for root-level pytest runs
from libs.sdk.python.tests.test_retry import *

