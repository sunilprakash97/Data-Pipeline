import subprocess

# Define the command to run the first script
cmd1 = ['python', 'script_1.py']

# Start the first script as a subprocess
p1 = subprocess.Popen(cmd1)

# Wait for both subprocesses to finish
p1.wait()


# Define the command to run the second script
cmd2 = ['python', 'script_2.py']

# Start the second script as a subprocess
p2 = subprocess.Popen(cmd2)

# Wait for both subprocesses to finish
p2.wait()


# Define the command to run the second script
cmd3 = ['python', 'FastAPI_Server.py']

# Start the second script as a subprocess
p3 = subprocess.Popen(cmd3)

# Wait for both subprocesses to finish
p3.wait()