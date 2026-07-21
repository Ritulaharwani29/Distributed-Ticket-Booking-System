import grpc
import sys
import os
import time
import uuid
import logging
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Generated gRPC code
import booking_pb2
import booking_pb2_grpc
import llm_pb2
import llm_pb2_grpc

from config import RAFT_NODES, LLM_SERVER_ADDRESS, SESSION_TTL_SECONDS

logging.basicConfig(
    level=logging.WARNING,    
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_NOT_LEADER_STATUSES = frozenset([
    booking_pb2.LoginResponse.Status.NOT_LEADER,
    booking_pb2.LogoutResponse.Status.NOT_LEADER,
    booking_pb2.GetAvailabilityResponse.Status.NOT_LEADER,
    booking_pb2.BookSeatResponse.Status.NOT_LEADER,
    booking_pb2.ChatbotResponse.Status.NOT_LEADER,
])


class BookingClient:
    def __init__(self, server_addresses, llm_address):
        self.server_addresses = server_addresses
        self.llm_address = llm_address
        self.current_leader = server_addresses[0] # Start with a guess
        
        self.token = None
        self.username = None
        self._token_expires_at = 0.0
        
        # Create a persistent gRPC channel for the LLM server
        self.llm_channel = grpc.insecure_channel(llm_address)
        self.llm_stub = llm_pb2_grpc.LLMServiceStub(self.llm_channel)


    def _get_stub(self, address: str):
        # helper to create a stub to a booking server
        channel = grpc.insecure_channel(address)
        return booking_pb2_grpc.BookingServiceStub(channel), channel


    def _call_rpc(self, rpc_name: str, request, retries: int = 2):
        for attempt in range(retries + 1):
            nodes_to_try = [self.current_leader] + [
                n for n in self.server_addresses if n != self.current_leader
            ]
 
            for node_addr in nodes_to_try:
                try:
                    stub, channel = self._get_stub(node_addr)
                    rpc_method    = getattr(stub, rpc_name)
                    response      = rpc_method(request, timeout=3.0)
                    channel.close()
 
                    # ── NOT_LEADER: follow the hint ──────────────────────
                    if (
                        hasattr(response, "status")
                        and response.status in _NOT_LEADER_STATUSES
                    ):
                        hint = getattr(response, "leader_hint", "")
                        if hint and hint in self.server_addresses:
                            logger.debug(
                                "%s is not leader. Redirecting to %s", node_addr, hint
                            )
                            self.current_leader = hint
                        else:
                            logger.debug(
                                "%s is not leader (no valid hint). Trying next node.", node_addr
                            )
                        continue    # try again with the new leader
 
                    # ── Success ──────────────────────────────────────────
                    self.current_leader = node_addr
                    return response
 
                except grpc.RpcError as exc:
                        channel.close()

                        print(f"\nRPC to {node_addr} failed")
                        print("Code    :", exc.code())
                        print("Details :", exc.details())

                        continue
 
            if attempt < retries:
                logger.debug("All nodes tried. Retrying in 1 s… (attempt %d/%d)", attempt + 1, retries)
                time.sleep(1.0)
 
        return None


    def _require_login(self) -> bool:
        # return True if the user has a valid session. Prints an error and returns False otherwise.
        if not self.token:
            print("Please log in first.")
            return False
        remaining = self._token_expires_at - time.time()
        if remaining <= 0:
            print("Your session has expired. Please log in again.")
            self.token    = None
            self.username = None
            self._token_expires_at = 0.0
            return False
        if remaining < 60:
            print(f"Session expires in {int(remaining)} seconds. Consider logging in again.")
        return True
    
   


    def login(self) :
        if self.token:
            print("You are already logged in.")
            return
            
        print("\n--- Login ---")
        username = input("Enter username: ")
        password = input("Enter password: ")
        request = booking_pb2.LoginRequest(username=username, password=password)
        
        response = self._call_rpc("Login", request)
        
        if response is None:
            print("Login failed: cluster is unavailable.")
        elif response.status == booking_pb2.LoginResponse.Status.SUCCESS:
            self.token      = response.token
            self.username   = username
            self._token_expires_at = time.time() + SESSION_TTL_SECONDS
            print(f"Welcome, {username}! Session valid for {SESSION_TTL_SECONDS // 60} minutes.")
        elif response.status == booking_pb2.LoginResponse.Status.INVALID_CREDENTIALS:
            print("Invalid username or password.")
        else:
            print(f"Login failed: {booking_pb2.LoginResponse.Status.Name(response.status)}")


    def logout(self):
        if not self._require_login():
            return
 
        request  = booking_pb2.LogoutRequest(token=self.token)
        response = self._call_rpc("Logout", request)
 
        if response and response.status == booking_pb2.LogoutResponse.Status.SUCCESS:
            print(f"Goodbye, {self.username}!")
            self.token             = None
            self.username          = None
            self._token_expires_at = 0.0
        else:
            print("Logout failed. Your session may have already expired.")
            
    ### ---------- Movie Catalogue --------------------
    MOVIES = {
        "1": {"title": "Shawshank Redemption",         "showtimes": ["10:00AM", "1:00PM"]},
        "2": {"title": "Star Wars: A New Hope",         "showtimes": ["11:30AM", "4:00PM"]},
        "3": {"title": "Mission: Impossible - Fallout", "showtimes": ["2:30PM",  "7:30PM"]},
        "4": {"title": "The Dark Knight",               "showtimes": ["9:00PM"]},
        "5": {"title": "The Godfather",                 "showtimes": ["5:00PM",  "8:30PM"]},
    }

    def _print_movie_list(self):
        print("\n--- Movies & Showtimes ---")
        for mid, data in self.MOVIES.items():
            print(f"  [{mid}] {data['title']}")
            print(f"       Showtimes: {', '.join(data['showtimes'])}")


    def get_availability(self):
        if not self._require_login():
            return
 
        self._print_movie_list()
        movie_id = input("\nEnter Movie ID: ").strip()
        if movie_id not in self.MOVIES:
            print("Invalid movie ID.")
            return
 
        showtime = input("Enter showtime: ").strip()
        request  = booking_pb2.GetAvailabilityRequest(
            token=self.token, movie_id=movie_id, showtime=showtime
        )
        response = self._call_rpc("GetAvailability", request)
 
        if response is None:
            print("Could not get availability: cluster is unavailable.")
            return
 
        if response.status == booking_pb2.GetAvailabilityResponse.Status.SUCCESS:
            title = self.MOVIES[movie_id]["title"]
            print(f"\n--- Seat Availability: {title} @ {showtime} ---")
            available = [s for s in response.seats if s.is_available]
            booked    = [s for s in response.seats if not s.is_available]
            print(f"  Available ({len(available)}): {', '.join(s.seat_id for s in available) or 'None'}")
            print(f"  Booked    ({len(booked)}):    {', '.join(s.seat_id for s in booked) or 'None'}")
        elif response.status == booking_pb2.GetAvailabilityResponse.Status.SHOWTIME_NOT_FOUND:
            print("Showtime not found. Check the movie ID and showtime and try again.")
        elif response.status == booking_pb2.GetAvailabilityResponse.Status.UNAUTHORIZED:
            print("Session invalid. Please log in again.")
        else:
            print(f"Error: {booking_pb2.GetAvailabilityResponse.Status.Name(response.status)}")

    def register(self):
        print("\n--- Register New Account ---")

        username = input("Choose a username: ").strip()
        password = input("Choose a password: ").strip()

        if not username or not password:
            print("Username and password cannot be empty.")
            return

        request = booking_pb2.RegisterRequest(
            username=username,
            password=password,
                )

        print("Submitting registration to the cluster...")

        response = self._call_rpc("Register", request)

        if response is None:
            print("Registration failed: cluster is unavailable.")

        elif response.status == booking_pb2.RegisterResponse.Status.SUCCESS:
            print("Registration successful!")
            print("You can now log in using your credentials.")

        elif response.status == booking_pb2.RegisterResponse.Status.USER_ALREADY_EXISTS:
            print("Username already exists. Please choose another username.")

        elif response.status == booking_pb2.RegisterResponse.Status.INVALID_REQUEST:
            print(response.message if response.message else "Invalid registration request.")

        elif response.status == booking_pb2.RegisterResponse.Status.NOT_LEADER:
            print("Request reached a follower. Retrying with leader...")

        else:
            print(
            f"Registration failed: "
            f"{booking_pb2.RegisterResponse.Status.Name(response.status)}")
            
    def book_seat(self):
        if not self._require_login():
            return
 
        self._print_movie_list()
        movie_id     = input("\nEnter Movie ID: ").strip()
        showtime     = input("Enter showtime: ").strip()
        seat_ids_str = input("Enter seat ID(s) to book (comma-separated, e.g. A1,A2): ").strip()
        seat_ids     = [s.strip().upper() for s in seat_ids_str.split(",") if s.strip()]
 
        if not seat_ids:
            print("❌ No seat IDs provided.")
            return
 
        request_id = str(uuid.uuid4())
        logger.debug("BookSeat request_id=%s seats=%s", request_id, seat_ids)
 
        request = booking_pb2.BookSeatRequest(
            token      = self.token,
            movie_id   = movie_id,
            showtime   = showtime,
            seat_ids   = seat_ids,
            request_id = request_id,
        )
 
        print("Submitting booking to the cluster…")
        response = self._call_rpc("BookSeat", request)
 
        if response is None:
            print("Booking failed: cluster is unavailable.")
        
        elif response.status == booking_pb2.BookSeatResponse.Status.SUCCESS:
            print(f"Booking confirmed! Booking ID: {response.booking_id}")
            print(f"   Seats: {', '.join(seat_ids)}  |  Movie ID: {movie_id}  |  Showtime: {showtime}")
        
        elif response.status == booking_pb2.BookSeatResponse.Status.ALREADY_BOOKED:
            print("Sorry, one or more of those seats were just taken by another user.")
            print(" Run 'Check availability' to see what's still free.")
        
        elif response.status == booking_pb2.BookSeatResponse.Status.SEAT_UNAVAILABLE:
            print("One or more seats are unavailable.")
        
        elif response.status == booking_pb2.BookSeatResponse.Status.UNAUTHORIZED:
            print("Session invalid. Please log in again.")
        
        else:
            print(f"Booking failed: {booking_pb2.BookSeatResponse.Status.Name(response.status)}")
        

    ### ------------ Chatbot ---------------------

    def view_my_bookings(self):
        if not self._require_login():
            return
 
        print("Fetching your bookings...")
        request  = booking_pb2.ChatbotRequest(
            token   = self.token,
            payload = "What are my bookings?",
            context = "",
        )
        response = self._call_rpc("AskChatbot", request)
 
        if response and response.status == booking_pb2.ChatbotResponse.Status.SUCCESS:
            print(f"\n  {response.answer}")
        elif response:
            print(f"Could not fetch bookings: {booking_pb2.ChatbotResponse.Status.Name(response.status)}")
        else:
            print("Cluster is unavailable.")



    def ask_chatbot(self):
        faq_questions = {
            "1": "What seats are available?",
            "2": "What payment methods are accepted?",
            "3": "What is the refund policy?",
            "4": "What are the timings of the movie?",
            "5": "How to cancel a booking?"
        }
        
        print("\n--- FAQ Chatbot ---")
        for key, question in faq_questions.items():
            print(f" {key}. {question}")
        print(" 6. Ask a custom question")
        
        choice = input("Select an option (1-6): ").strip()
        
        if choice in faq_questions:
            # General FAQ 
            print("Contacting chatbot...")
            try:
                llm_resp = self.llm_stub.GetLLMAnswer(
                    llm_pb2.GetLLMAnswerRequest(query=choice, context=""),
                    timeout=5.0,
                )
                print(f"\nChatbot: {llm_resp.answer}")
            except grpc.RpcError as exc:
                print(f"Chatbot unavailable: {exc.details()}")

        elif choice == '6':
            # Custom Question (Call Raft Leader)
            if not self._require_login():
                print("You must be logged in to ask custom questions about your account.")
                return

            query = input("Your question: ").strip()
            if not query:
                return

            print("Checking your account and contacting chatbot...")
            # This calls the leader-aware RPC, which will get context.
            request = booking_pb2.ChatbotRequest(token=self.token, payload=query, context="")
            response = self._call_rpc("AskChatbot", request) 
            
            if response and response.status == booking_pb2.ChatbotResponse.Status.SUCCESS:
                print(f"\nChatbot says: {response.answer}")
            elif response:
                print(f"Chatbot error: {booking_pb2.ChatbotResponse.Status.Name(response.status)}")
            else:
                print("Chatbot failed: Cluster is unavailable.")
        
        else:
            print("Invalid choice. Returning to main menu.")


    ## ---------- Main Menu --------------------
    def run_menu(self):
        while True:
            logged_in_str = f"(logged in as {self.username})" if self.username else "(not logged in)"
            
            print(f"  Distributed Ticket Booking System  {logged_in_str}")
            
            print("  1. Register")
            print("  2. Login")
            print("  3. Check seat availability")
            print("  4. Book a seat")
            print("  5. My bookings")
            print("  6. Ask the chatbot")
            print("  7. Logout")
            print("  8. Exit")
 
            choice = input("\nChoice (1-8): ").strip()
 
            if   choice == "1": self.register()
            elif choice == "2": self.login()
            elif choice == "3": self.get_availability()
            elif choice == "4": self.book_seat()
            elif choice == "5": self.view_my_bookings()
            elif choice == "6": self.ask_chatbot()
            elif choice == "7": self.logout()
            elif choice == "8":
                print("Goodbye!")
                self.llm_channel.close()
                break
            else:
                print("Invalid choice. Enter a number between 1 and 8.")
 
            if choice != "8":
                input("\nPress Enter to continue…")

if __name__ == '__main__':
    if "--debug" in sys.argv:
        logging.getLogger().setLevel(logging.DEBUG)
        print("[debug mode enabled]")
 
    client = BookingClient(RAFT_NODES, LLM_SERVER_ADDRESS)
    client.run_menu()