#!/usr/bin/env python
import sys
sys.path.insert(0, "c:\\Users\\nageshbhagelli\\OneDrive\\Desktop\\DBS\\dbs\\DBS-Lab-Project")

import logging
logging.basicConfig(level=logging.DEBUG)

try:
    from backend.app.main_new import app
    print("App imported successfully")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
