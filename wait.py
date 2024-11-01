"""wait.py - an example of busy-waiting."""
import threading
import time


def main():
    """Main thread, which spawns a second wait() thread."""
    print("main() starting")
    signals = {"shutdown": False}
    thread = threading.Thread(target=wait, args=(signals,))
    thread.start()
    time.sleep(1) # This gives up execution to the 'wait' thread
    # The shutdown variable will be set to true in approximately 1 second
    signals["shutdown"] = True
    thread.join()
    print("main() shutting down")


def wait(signals):
    """Wait for shutdown signal with sleep in between."""
    print("wait() starting")
    while not signals["shutdown"]:
        print("working")
        time.sleep(0.1)  # Uncomment to avoid busy-waiting
    print("wait() shutting down")


if __name__ == "__main__":
    main()
