#!/usr/bin/env python
import sys
import os

# Change to project directory
os.chdir("c:\\Users\\nageshbhagelli\\OneDrive\\Desktop\\DBS\\dbs\\DBS-Lab-Project")
sys.path.insert(0, ".")

from backend.app.main_new import app
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
