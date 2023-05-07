import uvicorn
from script_3 import app

if __name__ == "__main__":
    print("Starting the Server !!!")
    uvicorn.run(app, host="0.0.0.0", port=8000)
