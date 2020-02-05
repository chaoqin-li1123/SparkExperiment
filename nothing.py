import time
def detect_signal():
    filelist = os.listdir("/home/cc")
    for filename in filelist:
        if "end" in filename:
            return True
    return False


while True:
    if detect_signal():
        break
    time.sleep(0.5)