# Distributed Ticket Booking System using Raft Consensus

A distributed movie ticket booking system built using **Python**, **gRPC**, and the **Raft Consensus Algorithm**. The application uses three replicated booking servers to ensure fault tolerance and data consistency, along with a separate chatbot server to answer user queries.

---

## 🎥 Demo Videos

**Milestone 1:**
https://drive.google.com/drive/folders/19XfJFubtciIHLfzhAgukN4_EaI8X68tH?usp=sharing

**Milestone 2:**
https://drive.google.com/file/d/1ZDBQBzYnllnQ9FOKO00H5bZQuptnIzwa/view?usp=drive_link

---

## Features

* User Registration & Login
* Session-based Authentication
* Check Seat Availability
* Book Single or Multiple Seats
* View Booking History
* FAQ & Personalized Chatbot
* Leader Election using Raft
* Log Replication across Servers
* Fault Tolerant Distributed Architecture

---

## Tech Stack

* Python
* gRPC
* Protocol Buffers (protobuf)
* Raft Consensus Algorithm

---

## Installation

Install the required packages:

```bash
pip install -r requirements.txt
```

or

```bash
pip install grpcio grpcio-tools protobuf
```

---

## Project Structure

```text
Distributed-Ticket-Booking-System/
│
├── app_server/
├── client/
├── llm_server/
├── proto/
├── booking_pb2.py
├── booking_pb2_grpc.py
├── raft_pb2.py
├── raft_pb2_grpc.py
├── llm_pb2.py
├── llm_pb2_grpc.py
├── config.py
├── raft_log.py
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Running the Project

### Step 1: Remove old logs

**Windows (PowerShell)**

```powershell
Remove-Item *.json, *.jsonl -ErrorAction SilentlyContinue
```

**Linux/macOS**

```bash
rm *.json *.jsonl
```

---

### Step 2: Start the Servers

**Terminal 1 - LLM Server**

```bash
python llm_server/llm_server.py
```

**Terminal 2 - Application Server 1**

```bash
python app_server/app_server.py 50051 50051 localhost:50053 localhost:50054
```

**Terminal 3 - Application Server 2**

```bash
python app_server/app_server.py 50053 50053 localhost:50051 localhost:50054
```

**Terminal 4 - Application Server 3**

```bash
python app_server/app_server.py 50054 50054 localhost:50051 localhost:50053
```

**Terminal 5 - Client**

```bash
python client/client.py
```

---

## Ports Used

| Service      | Port  |
| ------------ | ----- |
| App Server 1 | 50051 |
| LLM Server   | 50052 |
| App Server 2 | 50053 |
| App Server 3 | 50054 |

---

## Client Menu

```
1. Register
2. Login
3. Check Seat Availability
4. Book Seat
5. My Bookings
6. Ask Chatbot
7. Logout
8. Exit
```

---

## Functionality

### Register

Create a new account using a username and password.

### Login

Authenticates the user and generates a session token for future requests.

### Check Seat Availability

Displays the available and booked seats for the selected movie and showtime.

### Book Seat

Books one or more seats and returns a unique booking ID after successful confirmation.

### My Bookings

Displays all bookings made by the logged-in user.

### Ask Chatbot

Provides answers to common booking-related questions and can also answer personalized queries based on the user's booking history.

Some example questions include:

* What seats are available?
* What payment methods are accepted?
* What is the refund policy?
* What are the movie timings?
* How do I cancel my booking?

### Logout

Ends the current user session.

### Exit

Closes the client application and disconnects from all servers.

---

## How Raft is Used

The booking servers form a Raft cluster where one server acts as the leader and the others act as followers.

* The leader handles all write requests.
* Booking operations are replicated to follower servers.
* A booking is confirmed only after it is replicated to the majority of servers.
* If the leader fails, a new leader is elected automatically, allowing the system to continue operating without losing data.

---

## Future Improvements

* Online payment integration
* Booking cancellation
* Seat reservation timeout
* Multiple theatre support
* Docker & Kubernetes deployment

---
