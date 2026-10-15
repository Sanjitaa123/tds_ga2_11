
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from collections import defaultdict, deque
from threading import Lock
from time import monotonic
from math import ceil
import base64
import json

app = FastAPI(title="Orders API")

# CORS: allow the grader's browser to call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TOTAL_ORDERS = 60
RATE_LIMIT = 17
WINDOW_SECONDS = 10

# Fixed catalog: IDs 1 through 60.
catalog = [
    {"id": i, "product": f"Product {i}", "quantity": 1}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory storage for idempotency keys and created orders.
idempotency_store = {}
created_orders = []
order_lock = Lock()
next_order_id = 1

# Independent sliding-window bucket for each client.
client_requests = defaultdict(deque)
rate_lock = Lock()


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # CORS preflight requests should not consume the client's quota.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = monotonic()

    with rate_lock:
        timestamps = client_requests[client_id]

        # Remove requests outside the last 10 seconds.
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1, ceil(WINDOW_SECONDS - (now - timestamps[0]))
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)

    return await call_next(request)


@app.get("/")
def home():
    return {"message": "Orders API is running"}


@app.post("/orders", status_code=201)
def create_order(
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1
    ),
):
    global next_order_id

    with order_lock:
        # A repeated key must return the original order.
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]

        order = {
            "id": f"created-{next_order_id}",
            "status": "created",
        }
        next_order_id += 1

        idempotency_store[idempotency_key] = order
        created_orders.append(order)

        return order


@app.get("/orders")
def get_orders(
    limit: int = Query(default=10, ge=1, le=60),
    cursor: str | None = Query(default=None),
):
    # The cursor encodes the next catalog offset.
    offset = 0

    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(
                cursor + "=" * (-len(cursor) % 4)
            ).decode()
            offset = int(decoded)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(status_code=400, detail="Invalid cursor")

    if offset < 0 or offset > TOTAL_ORDERS:
        raise HTTPException(status_code=400, detail="Invalid cursor")

    items = catalog[offset:offset + limit]
    next_offset = offset + len(items)

    next_cursor = None
    if next_offset < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(next_offset).encode()
        ).decode().rstrip("=")

    return {
        "items": items,
        "next_cursor": next_cursor,
    }
