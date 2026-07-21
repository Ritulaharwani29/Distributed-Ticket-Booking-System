# RAFT cluster 
RAFT_NODES = [
    "localhost:50051",
    "localhost:50053",
    "localhost:50054",
]

#  LLM / Chatbot server 
LLM_SERVER_ADDRESS = "localhost:50052"

#  Session management 
SESSION_TTL_SECONDS = 1800          # 30 minutes

# Raft timing (seconds) 
HEARTBEAT_INTERVAL   = 2.0      # leader heartbeat frequency
ELECTION_TIMEOUT_MIN = 5.0      # min follower wait before election
ELECTION_TIMEOUT_MAX = 10.0     # max follower wait before election

# Snapshot / log compaction 
SNAPSHOT_INTERVAL = 50      # take snapshot every N committed entries

# gRPC 
GRPC_MAX_WORKERS = 10