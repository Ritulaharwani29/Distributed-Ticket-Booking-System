import grpc
import threading
import random
import time
import logging
import sys
import os
import uuid
from concurrent import futures

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Generated gRPC modules
import raft_pb2
import raft_pb2_grpc
import booking_pb2
import booking_pb2_grpc
import llm_pb2
import llm_pb2_grpc

from raft_log import PersistentLog
from config import (
    LLM_SERVER_ADDRESS,
    SESSION_TTL_SECONDS,
    HEARTBEAT_INTERVAL,
    ELECTION_TIMEOUT_MIN,
    ELECTION_TIMEOUT_MAX,
    SNAPSHOT_INTERVAL,
    GRPC_MAX_WORKERS,
)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def get_initial_state():
    """Returns the initial replicated state machine."""
    return {
        "users": {
            "user1": "pass123",
            "user2": "mango07",
            "user3": "dot987",
        },
        "tokens": {},                 # Ephemeral session tokens
        "processed_requests": {},     # Idempotency tracking
        "movies": {
            "1": {
                "title": "Shawshank Redemption",
                "showtimes": {
                    "10:00AM": {
                        "seats": {
                            "A1": True,
                            "A2": True,
                            "B1": True,
                            "B2": True,
                        }
                    },
                    "1:00PM": {
                        "seats": {
                            "C1": True,
                            "C2": True,
                        }
                    },
                },
            },
            "2": {
                "title": "Star Wars: A New Hope",
                "showtimes": {
                    "11:30AM": {
                        "seats": {
                            "A1": True,
                            "A2": True,
                            "A3": True,
                            "A4": True,
                            "B1": True,
                        }
                    },
                    "4:00PM": {
                        "seats": {
                            "D1": True,
                            "D2": True,
                            "D3": True,
                        }
                    },
                },
            },
            "3": {
                "title": "Mission: Impossible - Fallout",
                "showtimes": {
                    "2:30PM": {
                        "seats": {
                            "E1": True,
                            "E2": True,
                        }
                    },
                    "7:30PM": {
                        "seats": {
                            "F1": True,
                            "F2": True,
                        }
                    },
                },
            },
            "4": {
                "title": "The Dark Knight",
                "showtimes": {
                    "9:00PM": {
                        "seats": {
                            "G1": True,
                            "G2": True,
                            "G3": True,
                        }
                    },
                },
            },
            "5": {
                "title": "The Godfather",
                "showtimes": {
                    "5:00PM": {
                        "seats": {
                            "B3": True,
                            "B4": True,
                            "B5": True,
                        }
                    },
                    "8:30PM": {
                        "seats": {
                            "F5": True,
                            "F6": True,
                            "F7": True,
                        }
                    },
                },
            },
        },
    }

# Define node states
class State:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
    _names    = {0: "FOLLOWER", 1: "CANDIDATE", 2: "LEADER"}
 
    @classmethod
    def name(cls, s):
        return cls._names.get(s, "UNKNOWN")

# Unified Server that handles both Raft and BookingService

class UnifiedServer(raft_pb2_grpc.RaftServicer, booking_pb2_grpc.BookingServiceServicer):

    def __init__(self, node_id: int, port: int, peer_addresses: list):
       
        self.node_id = node_id
        self.address = f"localhost:{port}"
        self.peer_addresses = peer_addresses
        self.majority = (len(self.peer_addresses) + 1) // 2 + 1
        
        # Concurrency
        self.lock = threading.Lock()
        self.commit_cv = threading.Condition(self.lock)
        self.pending_commands: dict = {} 

        # Raft's Persistent State 
        self.log_handler = PersistentLog(self.node_id)
        self.current_term = self.log_handler.current_term
        self.voted_for = self.log_handler.voted_for
        self.log = self.log_handler.log

        # Raft's Volatile State
        self.state = State.FOLLOWER
        self.leader_id = -1
        self.leader_hint = ""
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader's Volatile State
        self.next_index: dict = {}
        self.match_index: dict = {}

        # Application State
        self.state_machine: dict = {} 
        
        # Timers
        self.election_timer = None
        self.heartbeat_timer = None
        self._reset_election_timer()

       
        self.apply_thread = threading.Thread(target=self._apply_loop, daemon=True, name=f"node-{node_id}-apply")
        self.apply_thread.start()

        # LLM Client 
        self.llm_channel = grpc.insecure_channel(LLM_SERVER_ADDRESS)
        self.llm_stub = llm_pb2_grpc.LLMServiceStub(self.llm_channel)

        logger.info(
            "[Node %d] Initialized as FOLLOWER in term %d. "
            "Peers: %s  Majority needed: %d",
            node_id, self.current_term, peer_addresses, self.majority,
        )

    def _get_user_from_token(self, token: str):
        tokens = self.state_machine.setdefault("tokens", {})
        entry = tokens.get(token)

        if entry is None:
                return None

        if time.time() > entry.get("expires_at", 0):
                del tokens[token]
                logger.debug(
                    "[Node %d] Expired token removed",
                    self.node_id,
                )
                return None

        return entry.get("user")


    ## State machine
    def _apply_loop(self):
        # this loop continuously applies committed log entries to the state machine
        # On startup, apply all already-committed logs
        with self.lock:
            # try to restore from snapshot first
            snapshot_state = self.log_handler.load_snapshot_state()
            if snapshot_state:
                self.state_machine = snapshot_state
                self.last_applied  = self.log_handler.snapshot_index
                self.commit_index  = self.log_handler.snapshot_index
                logger.info(
                    "[Node %d SM] Restored from snapshot at index %d",
                    self.node_id, self.last_applied,
                )
            elif not self.log:
                # fresh node, no snapshot, no log
                self.state_machine = get_initial_state()
                logger.info("[Node %d SM] Initialized with fresh state.", self.node_id)
            else:
                # recovering without a snapshot — replay the full log
                logger.info("[Node %d SM] Replaying log to rebuild state…", self.node_id)
                self.state_machine = get_initial_state()
                for entry in self.log:
                    self._apply_command(entry.command)
                self.last_applied = len(self.log)
                self.commit_index = len(self.log)
                logger.info(
                    "[Node %d SM] State rebuilt. last_applied=%d",
                    self.node_id, self.last_applied,
                )
 
        while True:
            with self.lock:
                # sleep until there is something new to apply
                while self.commit_index <= self.last_applied:
                    self.commit_cv.wait()
 
                start_index    = self.last_applied + 1
                end_index      = self.commit_index
                entries_to_apply = self.log[start_index - 1 : end_index]
 
            for i, entry in enumerate(entries_to_apply):
                cmd_index = start_index + i
                self._apply_command(entry.command)
 
                with self.lock:
                    self.last_applied = cmd_index
                    # Wake any RPC handler waiting for this index to commit
                    if cmd_index in self.pending_commands:
                        self.pending_commands[cmd_index].notify_all()
                        del self.pending_commands[cmd_index]
 
            # After every SNAPSHOT_INTERVAL committed entries, take a snapshot to bound log growth. Only the leader does this to avoid races.
            with self.lock:
                entries_since_snapshot = (
                    self.last_applied - self.log_handler.snapshot_index
                )
                should_snapshot = (
                    self.state == State.LEADER
                    and entries_since_snapshot >= SNAPSHOT_INTERVAL
                )
                if should_snapshot:
                    snap_index = self.last_applied
                    snap_term  = self.log_handler.get_term(snap_index)
                    snap_state = {
                        k: v for k, v in self.state_machine.items()
                        if k != "tokens"   # tokens are ephemeral — don't persist
                    }
 
            if should_snapshot:
                self.log_handler.save_snapshot(snap_state, snap_index, snap_term)
                logger.info(
                    "[Node %d] Snapshot saved at index %d.", self.node_id, snap_index
                )


    def _apply_command(self, command: str) -> bool:
        
        try:
            parts = command.split()
            cmd_type = parts[0]
            
            if cmd_type == "REGISTER" and len(parts) == 3:
                _, user, pw = parts
                self.state_machine["users"][user] = pw
                return True
            
            elif cmd_type == "LOGIN" and len(parts) == 4:
                _, token, user, expires_at = parts 
                self.state_machine["tokens"][token] = {
                    "user": user,
                    "expires_at": float(expires_at),
                }
                return True
                
            elif cmd_type == "LOGOUT" and len(parts) == 2:
                _, token = parts
                self.state_machine["tokens"].pop(token, None)
                return True
                    
            elif cmd_type == "BOOK" and len(parts) >= 6:
                _, movie_id, showtime, user, request_id = parts[:5]
                seats_to_book = parts[5:]
 
                # Idempotency check - If we've already processed this request_id, skip booking
                if request_id in self.state_machine.get("processed_requests", {}):
                    logger.info(
                        "[Node %d SM] Duplicate BOOK request_id=%s — skipping.",
                        self.node_id, request_id,
                    )
                    return True  # idempotent success
 
                showtime_data = (
                    self.state_machine["movies"]
                    .get(movie_id, {})
                    .get("showtimes", {})
                    .get(showtime)
                )
                if showtime_data is None:
                    logger.warning(
                        "[Node %d SM] BOOK failed: movie/showtime not found.", self.node_id
                    )
                    return False
 
                # Check all seats are still free
                for seat in seats_to_book:
                    if showtime_data["seats"].get(seat) is not True:
                        logger.info(
                            "[Node %d SM] BOOK failed for %s: seat %s already taken.",
                            self.node_id, user, seat,
                        )
                        return False  # lost the race — another command got there first
 
                # All seats free — book them
                for seat in seats_to_book:
                    showtime_data["seats"][seat] = user
 
                # Record this request_id so duplicate RPCs are no-ops
                self.state_machine.setdefault("processed_requests", {})[request_id] = {
                    "user":     user,
                    "movie_id": movie_id,
                    "showtime": showtime,
                    "seats":    seats_to_book,
                }
                logger.info(
                    "[Node %d SM] Booked %s for user=%s movie=%s showtime=%s",
                    self.node_id, seats_to_book, user, movie_id, showtime,
                )
                return True

            else:
                logger.warning("[Node %d SM] Unknown command: %s", self.node_id, command)
                return False
                
        except Exception as e:
            logger.error(
                "[Node %d SM] Error applying '%s': %s", self.node_id, command, e
            )
            return False
            
    ## Command submission
    def _submit_command(self, command: str):
        # append command to the raft log and block until committed
        if self.state != State.LEADER:
            return (None, None)
            
        with self.lock:
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                command=command
            )
            
            # Append to local log
            self.log_handler.append_entry(new_entry)
            
            new_log_index = self.log_handler.get_last_log_index()
            logger.info(
                "[Node %d Leader] Command appended at index %d: %s",
                self.node_id, new_log_index, command.split()[0],
            )
            
            # Create a condition variable for this specific command
            commit_event = threading.Condition(self.lock)
            self.pending_commands[new_log_index] = commit_event
            
            # trigger heartbeat threads to replicate
            self.heartbeat_timer.cancel()
            self._send_heartbeats()
            
           
            if not commit_event.wait(timeout=5.0): # 5 second timeout
                logger.warning(
                    "[Node %d Leader] Timeout waiting for commit at index %d",
                    self.node_id, new_log_index,
                )
                self.pending_commands.pop(new_log_index, None)
                return (None, None)
            
            return (new_log_index, self.current_term)
        
    ## ------ LLM Context helper -------------
    def _gather_context_for_llm(self, user_id: str, query: str)->str :
        
        context = ""
        try:
            q = query.lower()
 
            if any(kw in q for kw in ("booking", "my seat", "ticket", "my ticket")):
                my_bookings = []
                for movie_data in self.state_machine["movies"].values():
                    title = movie_data["title"]
                    for showtime, st_data in movie_data["showtimes"].items():
                        for seat_id, booked_by in st_data["seats"].items():
                            if booked_by == user_id:
                                my_bookings.append(
                                    f"seat {seat_id} for '{title}' at {showtime}"
                                )
                context = (
                    f"User '{user_id}' has these bookings: {', '.join(my_bookings)}."
                    if my_bookings
                    else f"User '{user_id}' has no bookings yet."
                )
 
            elif any(kw in q for kw in ("available", "showtime", "free seat")):
                available = []
                for movie_data in self.state_machine["movies"].values():
                    title = movie_data["title"]
                    for showtime, st_data in movie_data["showtimes"].items():
                        free = [s for s, v in st_data["seats"].items() if v is True]
                        if free:
                            available.append(
                                f"{len(free)} seat(s) for '{title}' at {showtime}"
                            )
                        if len(available) >= 5:
                            break
                    if len(available) >= 5:
                        break
                context = (
                    f"Available options: {', '.join(available)}."
                    if available
                    else "No seats are currently available."
                )
 
        except Exception as exc:
            logger.error("[Node %d] Failed to gather LLM context: %s", self.node_id, exc)
            context = "Error retrieving availability data."
 
        return context


    #----------- BookingService gRPC Implementation ---------------
    def Login(self, request, context):
        with self.lock:
            if self.state != State.LEADER:
                return booking_pb2.LoginResponse(status=booking_pb2.LoginResponse.Status.NOT_LEADER, leader_hint=self.leader_hint)

            # Validate credentials 
            user = request.username
            if self.state_machine["users"].get(user) != request.password:
                return booking_pb2.LoginResponse(status=booking_pb2.LoginResponse.Status.INVALID_CREDENTIALS)
            
            
            token = str(uuid.uuid4())
            expires_at = time.time() + SESSION_TTL_SECONDS
            command = f"LOGIN {token} {user} {expires_at}"
            
       
        index, _ = self._submit_command(command)
        
        if index is not None:
            logger.info("[Node %d] User '%s' logged in.", self.node_id, user)
            return booking_pb2.LoginResponse(status=booking_pb2.LoginResponse.Status.SUCCESS, token=token, leader_hint=self.address)
        
        return booking_pb2.LoginResponse(status=booking_pb2.LoginResponse.Status.NOT_LEADER, leader_hint=self.leader_hint) 


    def Logout(self, request, context):
        with self.lock:
            if self.state != State.LEADER:
                return booking_pb2.LogoutResponse(status=booking_pb2.LogoutResponse.Status.NOT_LEADER)
            
            # Validate token
            if not self._get_user_from_token(request.token):
                return booking_pb2.LogoutResponse(status=booking_pb2.LogoutResponse.Status.INVALID_TOKEN)
            
            command = f"LOGOUT {request.token}"
            
        
        self._submit_command(command)
        
        return booking_pb2.LogoutResponse(status=booking_pb2.LogoutResponse.Status.SUCCESS)


    def GetAvailability(self, request, context):
        with self.lock:
            if self.state != State.LEADER:
                return booking_pb2.GetAvailabilityResponse(status=booking_pb2.GetAvailabilityResponse.Status.NOT_LEADER, leader_hint=self.leader_hint)

            # Validate
            if not self._get_user_from_token(request.token):
                return booking_pb2.GetAvailabilityResponse(status=booking_pb2.GetAvailabilityResponse.Status.UNAUTHORIZED)
            
            # Read from state machine
            movie_data = self.state_machine["movies"].get(request.movie_id)
            if not movie_data:
                return booking_pb2.GetAvailabilityResponse(status=booking_pb2.GetAvailabilityResponse.Status.SHOWTIME_NOT_FOUND)
            
            showtime_data = movie_data["showtimes"].get(request.showtime)
            if not showtime_data:
                return booking_pb2.GetAvailabilityResponse(status=booking_pb2.GetAvailabilityResponse.Status.SHOWTIME_NOT_FOUND)

            seats_list = [
                booking_pb2.Seat(seat_id=sid, is_available=(val is True))
                for sid, val in showtime_data["seats"].items()
            ]

            return booking_pb2.GetAvailabilityResponse(status=booking_pb2.GetAvailabilityResponse.Status.SUCCESS, seats=seats_list, leader_hint=self.address)
        
    
    def Register(self, request, context):
        with self.lock:
            print("Register called")
            print("Current node:", self.node_id)
            print("Current state:", self.state)

            if self.state != State.LEADER:
                print("Not leader. Leader hint:", self.leader_hint)
                return booking_pb2.RegisterResponse(
                    status=booking_pb2.RegisterResponse.Status.NOT_LEADER,
                    leader_hint=self.leader_hint
                )

            username = request.username.strip()
            password = request.password.strip()

            print("Submitting REGISTER command")

            command = f"REGISTER {username} {password}"

        print("Calling _submit_command()")

        index, term = self._submit_command(command)

        print("Submit finished:", (index, term))

        if index is None:
            return booking_pb2.RegisterResponse(
                status=booking_pb2.RegisterResponse.Status.INVALID_REQUEST,
                message="Failed to commit registration."
            )

        print("Returning SUCCESS")

        return booking_pb2.RegisterResponse(
            status=booking_pb2.RegisterResponse.Status.SUCCESS,
            message="Registration successful."
        )

    def BookSeat(self, request, context):
        with self.lock:
            if self.state != State.LEADER:
                return booking_pb2.BookSeatResponse(
                    status=booking_pb2.BookSeatResponse.Status.NOT_LEADER,
                    leader_hint=self.leader_hint,
                )
 
            user = self._get_user_from_token(request.token)
            if not user:
                return booking_pb2.BookSeatResponse(
                    status=booking_pb2.BookSeatResponse.Status.UNAUTHORIZED
                )
 
            # Idempotency check BEFORE submitting - if the client is retrying a request we already processed, return the same booking_id immediately.
            request_id = getattr(request, "request_id", None) or str(uuid.uuid4())
            existing   = self.state_machine.get("processed_requests", {}).get(request_id)
            if existing:
                logger.info(
                    "[Node %d] Idempotent re-submission for request_id=%s",
                    self.node_id, request_id,
                )
                # Reconstruct booking_id from the original term+index isn't
                # stored, so we return a stable derived ID.
                return booking_pb2.BookSeatResponse(
                    status=booking_pb2.BookSeatResponse.Status.SUCCESS,
                    booking_id=f"idem-{request_id[:8]}",
                    leader_hint=self.address,
                )
 
            # Optimistic availability check
            movie_data = self.state_machine["movies"].get(request.movie_id)
            if not movie_data:
                return booking_pb2.BookSeatResponse(
                    status=booking_pb2.BookSeatResponse.Status.SEAT_UNAVAILABLE
                )
            showtime_data = movie_data["showtimes"].get(request.showtime)
            if not showtime_data:
                return booking_pb2.BookSeatResponse(
                    status=booking_pb2.BookSeatResponse.Status.SEAT_UNAVAILABLE
                )
            for seat_id in request.seat_ids:
                if showtime_data["seats"].get(seat_id) is not True:
                    return booking_pb2.BookSeatResponse(
                        status=booking_pb2.BookSeatResponse.Status.SEAT_UNAVAILABLE
                    )
 
            # Build the log command, including the idempotency key
            seats_str = " ".join(request.seat_ids)
            command   = (
                f"BOOK {request.movie_id} {request.showtime} "
                f"{user} {request_id} {seats_str}"
            )
 
        index, term = self._submit_command(command)
 
        if index is None:
            return booking_pb2.BookSeatResponse(
                status=booking_pb2.BookSeatResponse.Status.NOT_LEADER,
                leader_hint=self.leader_hint,
            )
 
        # post-commit check: did WE win the race?
        with self.lock:
            won_race = request_id in self.state_machine.get("processed_requests", {})
 
        if won_race:
            booking_id = f"{term}-{index}"
            logger.info(
                "[Node %d] Booking confirmed: id=%s seats=%s user=%s",
                self.node_id, booking_id, list(request.seat_ids), user,
            )
            return booking_pb2.BookSeatResponse(
                status=booking_pb2.BookSeatResponse.Status.SUCCESS,
                booking_id=booking_id,
                leader_hint=self.address,
            )
        else:
            logger.info(
                "[Node %d] Booking lost race for seats %s (user=%s)",
                self.node_id, list(request.seat_ids), user,
            )
            return booking_pb2.BookSeatResponse(
                status=booking_pb2.BookSeatResponse.Status.ALREADY_BOOKED,
                leader_hint=self.address,
            )
        

    def AskChatbot(self, request, context):
        with self.lock:
            if self.state != State.LEADER:
                return booking_pb2.ChatbotResponse(
                    status=booking_pb2.ChatbotResponse.Status.NOT_LEADER, 
                    leader_hint=self.leader_hint
                )
            
            # Validate
            user_id = self._get_user_from_token(request.token)
            if not user_id:
                return booking_pb2.ChatbotResponse(
                    status=booking_pb2.ChatbotResponse.Status.UNAUTHORIZED
                )
            
            context_string = self._gather_context_for_llm(user_id, request.payload)

        
        try:
            llm_response = self.llm_stub.GetLLMAnswer(
                llm_pb2.GetLLMAnswerRequest(
                    query=request.payload, context=context_string
                ),
                timeout=5.0,
            )
            return booking_pb2.ChatbotResponse(
                status=booking_pb2.ChatbotResponse.Status.SUCCESS,
                answer=llm_response.answer,
            )
        
        except grpc.RpcError as e:
            logger.error("[Node %d] LLM call failed: %s", self.node_id, e)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details("Chatbot service is temporarily unavailable.")

        return booking_pb2.ChatbotResponse(
            status=booking_pb2.ChatbotResponse.Status.SUCCESS,
            answer="Sorry, the chatbot is temporarily unavailable. Please try again.",
        )

    ### Raft RPCs
    def RequestVote(self, request, context ):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.VoteReply(term=self.current_term, vote_granted=False)
 
            if request.term > self.current_term:
                self.log_handler.update_term(request.term)
                self.current_term = request.term
                self._become_follower(request.term)
 
            log_ok = (
                request.last_log_term  > self.log_handler.get_last_log_term()
                or (
                    request.last_log_term  == self.log_handler.get_last_log_term()
                    and request.last_log_index >= self.log_handler.get_last_log_index()
                )
            )
 
            vote_granted = False
            if (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok:
                vote_granted = True
                self.log_handler.update_term_and_vote(self.current_term, request.candidate_id)
                self.voted_for = request.candidate_id
                self._reset_election_timer()
                logger.info(
                    "[Node %d] Voted for %d in term %d",
                    self.node_id, request.candidate_id, self.current_term,
                )
 
            return raft_pb2.VoteReply(term=self.current_term, vote_granted=vote_granted)


    def AppendEntries(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

            if request.term > self.current_term:
                self.log_handler.update_term(request.term)
                self.current_term = request.term
                self._become_follower(request.term)
            
            if self.state == State.CANDIDATE:
                self._become_follower(request.term)

            self.leader_id = request.leader_id
            self.leader_hint = self._peer_address_for_id(request.leader_id) 
            self._reset_election_timer()
            
            # Consistency check
            if request.prev_log_index > 0:
                if self.log_handler.get_term(request.prev_log_index) != request.prev_log_term:
                    return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            
            new_entries_start = 0
            for i, entry in enumerate(request.entries):
                entry_index = request.prev_log_index + 1 + i
                if entry_index > self.log_handler.get_last_log_index():
                    new_entries_start = i
                    break
                if self.log_handler.get_term(entry_index) != entry.term:
                    self.log_handler.truncate_log(entry_index)
                    new_entries_start = i
                    break
            else:
                new_entries_start = len(request.entries)
 
            for entry in request.entries[new_entries_start:]:
                self.log_handler.append_entry(entry)
 
            if request.leader_commit > self.commit_index:
                self.commit_index = min(
                    request.leader_commit, self.log_handler.get_last_log_index()
                )
                self.commit_cv.notify()
 
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)
    

    ## -------- Raft State Machine -------
    
    def _peer_address_for_id(self, leader_id: int) -> str:
        """Return the address of the given leader ID."""
        if leader_id == self.node_id:
            return self.address

        for addr in self.peer_addresses:
            if addr.endswith(f":{leader_id}"):
                return addr

        return ""


    def _get_election_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)


    def _reset_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.cancel()

        self.election_timer = threading.Timer(
            self._get_election_timeout(),
            self._start_election,
        )
        self.election_timer.daemon = True
        self.election_timer.start()


    def _become_follower(self, term: int):
        """Transition this node to the FOLLOWER state."""

        logger.info(
            "[Node %d] is FOLLOWER (term %d)",
            self.node_id,
            term,
        )

        self.state = State.FOLLOWER
        self.current_term = term
        self.voted_for = self.log_handler.voted_for

        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

        self._reset_election_timer()


    def _start_election(self):
        with self.lock:
            if self.state == State.LEADER:
                return

            self.state = State.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id

            self.log_handler.update_term_and_vote(
                self.current_term,
                self.node_id,
            )

            self.votes_received = 1

            logger.info(
                "[Node %d] is CANDIDATE (term %d)",
                self.node_id,
                self.current_term,
            )

            self._reset_election_timer()

        for peer_addr in self.peer_addresses:
            threading.Thread(
                target=self._send_request_vote_rpc,
                args=(peer_addr, self.current_term),
                daemon=True,
            ).start()


    def _send_request_vote_rpc(self, peer_address, election_term):
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)

                response = stub.RequestVote(
                    raft_pb2.VoteRequest(
                        term=election_term,
                        candidate_id=self.node_id,
                        last_log_index=self.log_handler.get_last_log_index(),
                        last_log_term=self.log_handler.get_last_log_term(),
                    ),
                    timeout=1.0,
                )

                self._process_vote_reply(response, election_term)

        except grpc.RpcError:
            pass


    def _process_vote_reply(self, response, election_term):
        with self.lock:

            if response.term > self.current_term:
                self.log_handler.update_term(response.term)
                self.current_term = response.term
                self._become_follower(response.term)
                return

            if (
                self.state != State.CANDIDATE
                or self.current_term != election_term
            ):
                return

            if response.vote_granted:
                self.votes_received += 1

                if self.votes_received >= self.majority:
                    self._become_leader()


    def _become_leader(self):
        if self.state != State.CANDIDATE:
            return

        logger.info(
            "[Node %d] is the LEADER (term %d)",
            self.node_id,
            self.current_term,
        )

        self.state = State.LEADER
        self.leader_id = self.node_id
        self.leader_hint = self.address

        last_log_idx = self.log_handler.get_last_log_index()

        self.next_index = {
            peer: last_log_idx + 1
            for peer in self.peer_addresses
        }

        self.match_index = {
            peer: 0
            for peer in self.peer_addresses
        }

        if self.election_timer is not None:
            self.election_timer.cancel()

        self._send_heartbeats()


    def _update_leader_commit_index(self):
        last_log_idx = self.log_handler.get_last_log_index()

        for N in range(self.commit_index + 1, last_log_idx + 1):

            if self.log_handler.get_term(N) != self.current_term:
                continue

            match_count = 1

            for peer in self.peer_addresses:
                if self.match_index[peer] >= N:
                    match_count += 1

            if match_count >= self.majority:
                self.commit_index = N
                self.commit_cv.notify()
            else:
                break


    def _send_heartbeats(self):
        if self.state != State.LEADER:
            return

        for peer in self.peer_addresses:
            threading.Thread(
                target=self._send_append_entries_rpc,
                args=(peer,),
                daemon=True,
            ).start()

        self.heartbeat_timer = threading.Timer(
            HEARTBEAT_INTERVAL,
            self._send_heartbeats,
        )

        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()


    def _send_append_entries_rpc(self, peer_address):
        if self.state != State.LEADER:
            return

        with self.lock:

            next_idx = self.next_index[peer_address]
            prev_log_index = next_idx - 1
            prev_log_term = self.log_handler.get_term(prev_log_index)

            entries = self.log[next_idx - 1:]

            request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index,
            )

        try:
            with grpc.insecure_channel(peer_address) as channel:

                stub = raft_pb2_grpc.RaftStub(channel)

                response = stub.AppendEntries(
                    request,
                    timeout=1.0,
                )

            with self.lock:

                if self.state != State.LEADER:
                    return

                if response.term > self.current_term:
                    self.log_handler.update_term(response.term)
                    self.current_term = response.term
                    self._become_follower(response.term)
                    return

                if response.success:
                    new_next_index = prev_log_index + len(entries) + 1

                    self.next_index[peer_address] = new_next_index
                    self.match_index[peer_address] = new_next_index - 1

                    self._update_leader_commit_index()

                else:
                    self.next_index[peer_address] = max(
                        1,
                        self.next_index[peer_address] - 1,
                    )

        except grpc.RpcError:
            pass

## ----------------- Entry Point -----------------------
def serve(node_id: int, port: int, peer_addresses: list):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_MAX_WORKERS))
    
    unified_servicer = UnifiedServer(node_id, port, peer_addresses)
    
    raft_pb2_grpc.add_RaftServicer_to_server(unified_servicer, server)
    booking_pb2_grpc.add_BookingServiceServicer_to_server(unified_servicer, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info("=== Node %d started on port %d ===", node_id, port)
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\nStopping Node {node_id}...")
        if unified_servicer.election_timer: unified_servicer.election_timer.cancel()
        if unified_servicer.heartbeat_timer: unified_servicer.heartbeat_timer.cancel()
        server.stop(0)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python unified_server.py <node_id> <port> [peer_address1] [peer_address2] ...")
        print("Example (Node 50051): python unified_server.py 50051 50051 localhost:50053 localhost:50054")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    peers = sys.argv[3:]
    
    serve(node_id, port, peers)