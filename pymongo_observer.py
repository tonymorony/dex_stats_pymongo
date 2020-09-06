import time
import os
import pickle
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from watchdog.utils import dirsnapshot


def on_created(event):
    # pass to db_parser
    print(event.src_path)


def on_modified(event):
    # pass to db_parser
    print(event.src_path)


def make_snapshot(dpath):
    with open('snap_latest', 'wb') as sf:
        snap_latest = dirsnapshot.DirectorySnapshot(dpath, recursive=True)
        print(repr(snap_latest))
        pickle.dump(snap_latest, sf, -1)


def check_snapshot(dpath, old_snap_exists=True):
    snap_current = dirsnapshot.DirectorySnapshot(dpath, recursive=True)
    snap_prev = {}
    if old_snap_exists:
        with open('snap_latest', 'rb') as sg:
            snap_prev = pickle.load(sg)
    else:
        snap_prev = dirsnapshot.EmptyDirectorySnapshot()
    check = dirsnapshot.DirectorySnapshotDiff(snap_prev, snap_current)

    for mods in check.files_modified:
        print(mods)     # pass to db_parser
    for new in check.files_created:
        print(new)      # pass to db_parser


if __name__ == "__main__":
    patterns = "*.json"
    ignore_patterns = ""
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    my_event_handler.on_created = on_created
    my_event_handler.on_modified = on_modified
    path = "../SWAPS/STATS/MAKER/"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)
    my_observer.start()
    snapshot_found = False
    if os.path.isfile('snap_latest'):
        snapshot_found = True
    check_snapshot(path, snapshot_found)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
        my_observer.join()
    make_snapshot(path)
