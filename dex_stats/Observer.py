import os
import time
import pickle
from Parser import Parser
from watchdog.events import (FileCreatedEvent, 
                             FileModifiedEvent)
from watchdog.observers import Observer as Watchdog
from watchdog.events import PatternMatchingEventHandler
from watchdog.utils.dirsnapshot import (DirectorySnapshot, 
                                        DirectorySnapshotDiff,
                                        EmptyDirectorySnapshot)



class Observer(Watchdog):
    def __init__(self,
                 snap_path="../../dex_stats_pymongo-data/STATS/MAKER/",
                 mask=".json",
                 *args, **kwargs):
        self.snap_path = snap_path
        self.mask = mask
        self.path = ""
        Watchdog.__init__(self, *args, **kwargs)


    def start_watchdog(self):
        if os.path.exists(self.snap_path):
            with open(self.snap_path, 'rb') as f:
                pre_snap = pickle.load(f)
        else:
            pre_snap = EmptyDirectorySnapshot()

        for watcher, handler in self._handlers.items():  # dict { watcher: handler }
            self.path = watcher.path  # we have single watcher: handler item, thus it works
            curr_snap = DirectorySnapshot(path)
            diff = DirectorySnapshotDiff(pre_snap, curr_snap)
            for h in handler:
                for new_path in diff.files_created:
                    if self.mask in new_path:
                        h.dispatch(FileCreatedEvent(new_path))
                for mod_path in diff.files_modified:
                    if self.mask in mod_path:
                        h.dispatch(FileModifiedEvent(mod_path))
        Watchdog.start(self)


    def stop_watchdog(self):
        snapshot = DirectorySnapshot(self.path)
        with open(self.snap_path, 'wb') as fb:
            pickle.dump(snapshot, fb, -1)
        Watchdog.stop(self)



def on_created(event):
    if ".json" in event.src_path:
        parser.insert_into_swap_collection(event.src_path)


def on_modified(event):
    if ".json" in event.src_path:
        parser.insert_into_swap_collection(event.src_path)


if __name__ == "__main__":
    path = "../../dex_stats_pymongo-data/STATS/MAKER/"
    folder_snapshot_path = path + "backup.pickle"
    Parser = Parser(swaps_folder_path=path)
    pattern = "*.json"

    event_handler = PatternMatchingEventHandler(patterns=pattern,
                                                ignore_directories=True, 
                                                case_sensitive=True)
    event_handler.on_created = on_created
    event_handler.on_modified = on_modified

    observer = Observer(snap_path=folder_snapshot_path, 
                        mask=".json")
    observer.schedule(event_handler, 
                      path, 
                      recursive=True)
    snapshot_found = False
    print("Starting observer")
    observer.start_watchdog()
    print("Observer started")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop_watchdog()
    observer.join()
    Parser.insert_into_parsed_files_collection()
