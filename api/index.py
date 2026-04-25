import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import database as db
db.init_db()

from app import app
