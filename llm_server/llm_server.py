import grpc
from concurrent import futures
import time
import uuid
import sys
import os
import logging

# Adds the project's root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import llm_pb2
import llm_pb2_grpc

from config import LLM_SERVER_ADDRESS, GRPC_MAX_WORKERS
 
# ── Logging setup ─────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

INTENT_MY_BOOKINGS  = "MY_BOOKINGS"
INTENT_AVAILABILITY = "AVAILABILITY"
INTENT_CANCEL       = "CANCEL"
INTENT_PAYMENT      = "PAYMENT"
INTENT_REFUND       = "REFUND"
INTENT_TIMING       = "TIMING"
INTENT_FAQ_ID       = "FAQ_ID"  
INTENT_UNKNOWN      = "UNKNOWN"

class LLMServiceServicer(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        self.faqs = {
            "1": "For real-time seat availability, please use the 'Get seat availability' feature after logging in. This will show you all available seats for a specific showtime.",
            "2": "We accept all major credit cards, as well as several digital payment methods. You will be prompted to enter your payment details during the booking process.",
            "3": "To get a refund, you must cancel your ticket at least 24 hours before the showtime. Refunds are processed within 5-7 business days.",
            "4": "The show times can be found on the movie's details page. Our system will display this information before you select your seats.",
            "5": "Currently, cancellations are handled by contacting customer support."
        }

    INTENT_RULES = [
        (["my booking", "my ticket", "my seat", "what did i book", "show my"], INTENT_MY_BOOKINGS),
        (["available", "free seat", "which seat", "seat available", "showtime"], INTENT_AVAILABILITY),
        (["cancel", "cancellation", "cancel my"], INTENT_CANCEL),
        (["payment", "pay", "credit card", "debit card", "upi", "wallet"], INTENT_PAYMENT),
        (["refund", "money back", "return", "reimburs"], INTENT_REFUND),
        (["timing", "time", "when", "schedule", "show start"], INTENT_TIMING),
    ]

    def _detect_intent(self, query: str) -> str:
        stripped = query.strip()

        # Fast path: client sent a numeric FAQ menu choice ("1" … "5")
        if stripped in self.faqs:
            return INTENT_FAQ_ID

        q = query.lower()

        for keywords, intent in self.INTENT_RULES:
            if any(kw in q for kw in keywords):
                return intent

        return INTENT_UNKNOWN

    def _build_response(self, intent: str, query: str, context: str) -> str:
        """
        Build a natural-language answer given an intent,
        original query, and optional context.
        """

        if intent == INTENT_FAQ_ID:
            return self.faqs.get(
                query.strip(),
                "Sorry, I don't have an answer for that."
            )

        if intent == INTENT_MY_BOOKINGS:
            if context:
                return (
                    f"Here's what I found for your account: {context} "
                    f"If you'd like to cancel or have questions about a booking, "
                    f"please contact customer support with your Booking ID."
                )

            return (
                "I couldn't retrieve your bookings right now—you may need to "
                "log in first. Use the 'My bookings' option from the main menu "
                "to see your confirmed reservations."
            )

        if intent == INTENT_AVAILABILITY:
            if context:
                return (
                    f"{context} "
                    f"To book a seat, use the 'Book a seat' option from the main menu "
                    f"and enter the movie ID, showtime, and seat ID."
                )

            return self.faqs["1"]

        if intent == INTENT_CANCEL:
            return self.faqs["5"]

        if intent == INTENT_PAYMENT:
            return self.faqs["2"]

        if intent == INTENT_REFUND:
            return self.faqs["3"]

        if intent == INTENT_TIMING:
            return self.faqs["4"]

        return (
            "I'm sorry, I didn't quite understand that. You can ask me about:\n"
            "  - Seat availability\n"
            "  - Your bookings\n"
            "  - Payment methods\n"
            "  - Refund policy\n"
            "  - How to cancel a booking\n"
            "  - Movie showtimes\n"
            "Or choose a numbered option (1–5) from the FAQ menu."
        )

    def GetLLMAnswer(self, request, context):
        try:
            query = request.query or ""
            ctx = request.context or ""

            intent = self._detect_intent(query)

            logger.info(
                "Query: %r | Context provided: %s | Intent: %s",
                query[:80],
                bool(ctx),
                intent,
            )

            answer = self._build_response(intent, query, ctx)

            logger.debug("Answer: %s", answer[:120])

            return llm_pb2.GetLLMAnswerResponse(answer=answer)

        except Exception as exc:
            logger.error(
                "Unexpected error in GetLLMAnswer: %s",
                exc,
                exc_info=True,
            )

            return llm_pb2.GetLLMAnswerResponse(
                answer=(
                    "I'm sorry, something went wrong on my end. "
                    "Please try again in a moment."
                )
            )
  
        

def serve():
    port   = LLM_SERVER_ADDRESS.split(":")[-1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_MAX_WORKERS))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.warning("=== LLM Server started on port %s ===", port)
    logger.warning(
        "Design note: This server is intentionally OUTSIDE the Raft cluster "
        "(read-only; context is injected by the Raft leader)."
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.warning("LLM Server shutting down…")
        server.stop(0)

if __name__ == '__main__':
    if "--debug" in sys.argv:
        logging.getLogger().setLevel(logging.DEBUG)
        print("[debug mode enabled]")

    serve()