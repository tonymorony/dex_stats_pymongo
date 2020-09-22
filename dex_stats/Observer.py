import os
import sys
import time
import pickle
import logging

from Parser import Parser
from datetime import datetime, timedelta
from watchdog.events import (FileCreatedEvent,
                             FileModifiedEvent,
                             FileSystemEventHandler)
from watchdog.observers import Observer as Watchdog
from watchdog.events import PatternMatchingEventHandler
from watchdog.utils.dirsnapshot import (DirectorySnapshot,
                                        DirectorySnapshotDiff,
                                        EmptyDirectorySnapshot)



class Observer(Watchdog):
    def __init__(self,
                 mask=".json",
                 snap_path="/Users/dathbezumniy/kmd-qa/dex_stats-data/STATS/MAKER/",
                 *args, **kwargs):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.snap_path = path
        self.mask = mask
        self.path = ""
        Watchdog.__init__(self, *args, **kwargs)


    def start(self):
        '''
        if os.path.exists(self.snap_path):
            with open(self.snap_path, 'rb') as f:
                pre_snap = pickle.load(f)
        else:
        '''
        pre_snap = EmptyDirectorySnapshot()

        for watcher, handler in self._handlers.items():  # dict { watcher: handler }
            self.path = watcher.path  # we have single watcher: handler item, thus it works
            curr_snap = DirectorySnapshot(path)
            diff = DirectorySnapshotDiff(pre_snap, curr_snap)
            for h in handler:
                for new_path in diff.files_created:
                    if self.mask in new_path:
                        h.dispatch(FileCreatedEvent(new_path))
                '''
                for mod_path in diff.files_modified:
                    if self.mask in mod_path:
                        h.dispatch(FileModifiedEvent(mod_path))
                '''
        Observer.start(self)


    def stop(self):
        snapshot = DirectorySnapshot(self.path)
        with open(self.snap_path, 'wb') as f:
            pickle.dump(snapshot, f, -1)
        Watchdog.stop(self)




class MyHandler(FileSystemEventHandler):
    def __init__(self):
        self.started_at = datetime.now()


    def on_created(self, event):
        logging.debug(f'Event type: {event.event_type}  path : {event.src_path}')
        parser.insert_into_swap_collection(event.src_path)

    '''
    def on_modified(self, event):
        if datetime.now() - self.started_at < timedelta(hours=1):
            return

        logging.debug(f'Event type: {event.event_type}  path : {event.src_path}')
        parser.insert_into_swap_collection(event.src_path)
    '''



if __name__ == "__main__":
    path = "/Users/dathbezumniy/kmd-qa/dex_stats-data/STATS/MAKER/"
    snap_path = path + "backup.pickle"
    parser = Parser(swaps_folder_path=path)
    pattern = "*.json"

    observer = Observer(snap_path=snap_path,
                        mask=".json")
    observer.schedule(MyHandler(),
                      path,
                      recursive=False)

    logging.debug("Starting observer")
    observer.start()
    logging.debug("Observer started")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    finally:
        parser.insert_into_parsed_files_collection()
        parser.insert_into_unique_pairs_collection()
        observer.join()
