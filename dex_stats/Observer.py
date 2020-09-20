import os
import time
import pickle
import logging

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
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.snap_path = snap_path
        self.snap_backup = snap_path + "backup.pickle"
        self.parser  = Parser()
        self.pattern = "*.json"

        setup_event_handler()


        self.mask = mask
        self.path = ""
        Watchdog.__init__(self, *args, **kwargs)


    def setup_event_handler(self):
        self.event_handler = PatternMatchingEventHandler(patterns=self.pattern,
                                                         ignore_directories=True, 
                                                         case_sensitive=True)
        event_handler.on_created  = self.on_created
        event_handler.on_modified = self.on_modified


    def get_pre_snap(self):
        if os.path.exists(self.snap_backup):
            with open(self.snap_backup, 'rb') as f:
                return pickle.load(f), True
        else:
            return EmptyDirectorySnapshot(), False


    def start_watchdog(self):
        pre_snap = self.get_pre_snap()
        for watcher, handler in self._handlers.items():  # dict { watcher: handler }
            self.path = watcher.path  # we have single watcher: handler item, thus it works
            curr_snap = DirectorySnapshot(path)
            diff = DirectorySnapshotDiff(pre_snap, curr_snap)
            for handle in handler:
                for new_path in diff.files_created:
                    if self.mask in new_path:
                        handle.dispatch(FileCreatedEvent(new_path))
                for mod_path in diff.files_modified:
                    if self.mask in mod_path:
                        handle.dispatch(FileModifiedEvent(mod_path))
        Watchdog.start(self)


    def stop_watchdog(self):
        snapshot = DirectorySnapshot(self.path)
        with open(self.snap_path, 'wb') as f:
            pickle.dump(snapshot, f, -1)
        Watchdog.stop(self)


    def on_created(event):
        if ".json" in event.src_path: # ???
            parser.insert_into_swap_collection(event.src_path)


    def on_modified(event):
        if ".json" in event.src_path: # ???
            parser.insert_into_swap_collection(event.src_path)


if __name__ == "__main__":
    observer = Observer()
    observer.schedule(event_handler,
                      path,
                      recursive=True)

    logging.debug("Starting observer")
    observer.start_watchdog()
    logging.debug("Observer started")

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        observer.stop_watchdog()
    observer.join()

    parser.insert_into_parsed_files_collection()
