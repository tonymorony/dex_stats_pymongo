import time
import os
import pickle
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from watchdog.utils import dirsnapshot
from db_parser import DB_Parser as dbpar

path = r"..\SWAPS\STATS\MAKER\\"
Parser = dbpar(swaps_folder_path=path)


def on_created(event):
    Parser.insert_into_swap_collection(event.src_path)
    print(event.src_path)


def on_modified(event):
    Parser.insert_into_swap_collection(event.src_path)
    print(event.src_path)


def make_snapshot(dpath):
    with open('snap_latest', 'wb') as sf:
        snap_latest = dirsnapshot.DirectorySnapshot(dpath, recursive=True)
        print(repr(snap_latest))
        pickle.dump(snap_latest, sf, -1)


def check_snapshot(dpath, old_snap_exists=True):
    print("checking snapshot step1")
    snap_current = dirsnapshot.DirectorySnapshot(dpath, recursive=True)
    snap_prev = {}
    if old_snap_exists:
        with open('snap_latest', 'rb') as sg:
            snap_prev = pickle.load(sg)
    else:
        snap_prev = dirsnapshot.EmptyDirectorySnapshot()
    print("checking snapshot step2")
    check = dirsnapshot.DirectorySnapshotDiff(snap_prev, snap_current)

    for mods in check.files_modified:
        Parser.insert_into_swap_collection(mods)
        print(mods)
    for new in check.files_created:
        Parser.insert_into_swap_collection(new)
        print(new)


if __name__ == "__main__":
    print("Started")
    patterns = "*.json"
    ignore_patterns = ""
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    my_event_handler.on_created = on_created
    my_event_handler.on_modified = on_modified
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)
    snapshot_found = False
    print("checking snapshot step0")
    if os.path.isfile('snap_latest'):
        snapshot_found = True
    my_observer.start()
    print("Observer started")
    check_snapshot(path, snapshot_found)
    print("checking snapshot step4 - end")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
    my_observer.join()
    make_snapshot(path)
