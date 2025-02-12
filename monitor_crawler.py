import subprocess
import time

def run_crawler():
    print("🔄 Starting the crawler...")

    # Run launch.py and capture output
    process = subprocess.Popen(["python3", "launch.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    last_output_time = time.time()  # Record the last time output was received
    timeout = 180  # Timeout in seconds; if no output for 180 seconds, assume the server is down

    while True:
        output = process.stdout.readline()  # Read a line of output
        if output:
            print("🐍 Crawler output:", output.strip())  # Print output in real-time
            last_output_time = time.time()  # Update last output time
        
        # Check if timeout has been exceeded (server may be down)
        if time.time() - last_output_time > timeout:
            print("⚠️ Server might be down, waiting for recovery...")
            process.terminate()  # Terminate the stuck process
            process.wait()  # Ensure the process has fully exited
            time.sleep(60)  # Wait for 60 seconds before retrying
            return True  # Retry the process

        # Check if the crawler has exited on its own
        if process.poll() is not None:
            print("⏳ Crawler has exited, waiting for the server to start...")
            time.sleep(120)  # Wait for 120 seconds before retrying
            return True  # Retry the process

if __name__ == "__main__":
    while True:  # Infinite loop until the server recovers
        restart = run_crawler()
        if not restart:
            break  # Exit if retry is not needed
        print("🔁 Server not recovered yet, continuing to monitor...")
