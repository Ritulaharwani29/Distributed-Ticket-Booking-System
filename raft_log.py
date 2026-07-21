# raft_log.py
import json
import logging
import os
import threading

# We need the LogEntry message definition
import raft_pb2

logger = logging.getLogger(__name__)

class PersistentLog:
    def __init__(self, node_id: int):
        self.node_id = node_id
        # A lock to protect file access from concurrent threads
        self._lock = threading.Lock()
        
        # File paths for persistence
        self._metadata_file = f"node_{node_id}_metadata.json"
        self._log_file = f"node_{node_id}_log.jsonl" # JSON Lines format
        self._snapshot_file = f"node_{node_id}_snapshot.json" # for future snapshotting
        
        # Raft persistent state (survives crashes)
        self.current_term: int = 0
        self.voted_for = None
        self.log: list = [] # List of raft_pb2.LogEntry objects

        # Raft volatile state
        self.commit_index: int = 0 # index of highest log entry known to be committed
        self.last_applied: int = 0 # index of highest log entry applied to state machine

        # Snapshot state
        self.snapshot_index: int = 0 # index of last entry included in the snapshots
        self.snapshot_term: int = 0 # term of last entry included in the snapshots

        # Load persistent state from disk
        self._load_snapshot()
        self._load_metadata()
        self._load_log()
        logger.info(
            "[Node %s] PersistentLog loaded. term=%d voted_for=%s "
            "log_size=%d snapshot_index=%d commit_index=%d",
            node_id, self.current_term, self.voted_for,
            len(self.log), self.snapshot_index, self.commit_index,
        )

    

    def _load_metadata(self):
        # Loads current_term and voted_for from the metadata file.
        try:
            with open(self._metadata_file, 'r') as f:
                data = json.load(f)
                self.current_term = data.get('current_term', 0)
                self.voted_for = data.get('voted_for', None)
        except (IOError, json.JSONDecodeError):
            # File doesn't exist or is corrupt, start fresh
            self.current_term = 0
            self.voted_for = None
            self._save_metadata() # Create the file

    def _save_metadata(self):
        #Saves current_term and voted_for to the metadata file
        tmp = self._metadata_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(
                {"current_term": self.current_term, "voted_for": self.voted_for},
                f, indent=2,
            )
            f.flush()
            os.fsync(f.fileno())  # flush page cache to physical disk
        os.replace(tmp, self._metadata_file)
        


    def update_term_and_vote(self, term: int, voted_for):
        with self._lock:
            self.current_term = term
            self.voted_for = voted_for
            self._save_metadata()

    def update_term(self, term: int):
        with self._lock:
            self.current_term = term
            self.voted_for = None # New term means we haven't voted yet
            self._save_metadata()

   

    def _load_log(self):
        #Loads log entries from the JSON-Lines file
        self.log = []
        try:
            with open(self._log_file, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    entry = raft_pb2.LogEntry(term=data['term'], command=data['command'])
                    self.log.append(entry)
        except (IOError, json.JSONDecodeError):
            # File doesn't exist or is empty
            pass 

    def append_entry(self, entry):
        # Appends a single log entry to the log and persists it
        with self._lock:
            self.log.append(entry)
            # Append to file (JSON Lines)
            
            with open(self._log_file, 'a') as f:
                f.write(json.dumps({'term': entry.term, 'command': entry.command}) + '\n')
    
    def get_last_log_index(self) -> int:
        #Raft log index is 1-based.
        return self.snapshot_index + len(self.log)

    def get_last_log_term(self) -> int:
        #Returns the term of the last log entry
        if self.log:
            return self.log[-1].term
        return self.snapshot_term # If no log entries, return snapshot term
    
    

    def get_entry(self, index: int):
        """
        Get log entry at a 1-based index.
        Returns None if index is out of bounds.
        """
        local = index - self.snapshot_index
        if 1 <= local <= len(self.log):
            return self.log[local - 1] # Convert 1-based to 0-based
        return None
        

    def get_term(self, index: int) -> int:
        """
        Get term of entry at a 1-based index.
        Returns 0 if index is 0 (snapshot) or out of bounds.
        """
        if index == 0:
            return 0 
        if index == self.snapshot_index:
            return self.snapshot_term
        entry = self.get_entry(index)
        return entry.term if entry else 0
        
    def truncate_log(self, from_index: int):
        # truncates the log *from* a 1-based index.
        
        with self._lock:
            local_from = from_index - self.snapshot_index   # local 1-based
 
            if local_from < 1:
                logger.warning(
                    "[Node %s] truncate_log(%d) is at/before snapshot boundary — ignored",
                    self.node_id, from_index,
                )
                return
 
            if local_from > len(self.log):
                return  # nothing to truncate
 
            logger.info(
                "[Node %s] Truncating log from global index %d", self.node_id, from_index
            )
            self.log = self.log[: local_from - 1]
 
            # Atomic rewrite
            tmp = self._log_file + ".tmp"
            with open(tmp, "w") as f:
                for e in self.log:
                    f.write(json.dumps({"term": e.term, "command": e.command}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._log_file)


    ### Commit index and last applied index
    def advance_commit_index(self, new_commit_index: int):
        # move commit_index forward
        with self._lock:
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index
                logger.debug(
                    "[Node %s] commit_index → %d", self.node_id, self.commit_index
                )
 
    def entries_to_apply(self) -> list:
        # return all committed but not yet applied entries in the range (last_applied, commit_index] inclusive.
        
        with self._lock:
            start = self.last_applied - self.snapshot_index 
            end   = self.commit_index  - self.snapshot_index
            return list(self.log[start:end])
 
    def mark_applied(self, index: int):
        # mark a log index as applied to the state machine
        with self._lock:
            if index > self.last_applied:
                self.last_applied = index
 
   
    ### SNAPSHOTS  (log compaction)
    
    def _load_snapshot(self):
        # load snapshot metadata from disk (if a snapshot exists)
        try:
            with open(self._snapshot_file, "r") as f:
                data = json.load(f)
            self.snapshot_index = data.get("snapshot_index", 0)
            self.snapshot_term  = data.get("snapshot_term",  0)
            # after a snapshot, volatile indices start at the snapshot boundary.
            self.commit_index   = self.snapshot_index
            self.last_applied   = self.snapshot_index
            logger.info(
                "[Node %s] Loaded snapshot up to index %d (term %d)",
                self.node_id, self.snapshot_index, self.snapshot_term,
            )
        except (IOError, json.JSONDecodeError):
            self.snapshot_index = 0
            self.snapshot_term  = 0
 
    def save_snapshot(
        self,
        state_machine_state: dict,
        last_included_index: int,
        last_included_term:  int,
    ):

        #Persist a state-machine snapshot and compact the log.
        with self._lock:
            # write snapshot atomically
            tmp = self._snapshot_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(
                    {
                        "snapshot_index":      last_included_index,
                        "snapshot_term":       last_included_term,
                        "state_machine_state": state_machine_state,
                    },
                    f, indent=2,
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._snapshot_file)
 
            # Compact in-memory log
            cutoff = last_included_index - self.snapshot_index
            if cutoff > 0:
                self.log = self.log[cutoff:]
            self.snapshot_index = last_included_index
            self.snapshot_term  = last_included_term
 
            # Rewrite log file atomically (without compacted entries)
            tmp = self._log_file + ".tmp"
            with open(tmp, "w") as f:
                for e in self.log:
                    f.write(json.dumps({"term": e.term, "command": e.command}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._log_file)
 
            logger.info(
                "[Node %s] Snapshot saved at index %d. Log compacted to %d entries.",
                self.node_id, last_included_index, len(self.log),
            )
 
    def load_snapshot_state(self) -> dict:
        # load the state machine state from the snapshot file
        try:
            with open(self._snapshot_file, "r") as f:
                data = json.load(f)
            return data.get("state_machine_state", {})
        except (IOError, json.JSONDecodeError):
            return {}
 