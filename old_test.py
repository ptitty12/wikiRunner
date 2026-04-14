import os
from datetime import datetime

timestamp = datetime.now().isoformat()

with open(r"C:\Users\Patrick Taylor\PycharmProjects\wikiSpeedrunner\wiki.db-wal", "w") as f:
    f.write(f"hardcoded path write: {timestamp}\n")

with open(os.path.join(os.getcwd(), "im_here"), "w") as f:
    f.write(f"cwd: {os.getcwd()}\ntime: {timestamp}\n")