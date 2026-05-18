# AOAO Trading

Event-driven algorithmic trading platform for crypto spot markets. We ingest live exchange data, run multiple strategy instances in parallel, route orders through a shared OMS, and close the loop with portfolio state back into every strategy.



## Architecture

The diagram above is the current system map (`aoao_schema.png`, replacing the older `architecture.png`). Exchanges sit on the left, Kafka-backed services in the middle, portfolio and UI on the right. FlatBuffers schemas in `**shared**` define every wire message between C++ and Rust.

```
Binance WS
  -> md_reader_* -> md_mq
  -> strategy_* -> order_mq -> order_gateway -> oms_sink_mq -> OMS
  -> portfolio_engine -> position_updates -> strategy_* + portfolio_monitor
  -> order_status_stream_handler -> oms_flags_mq ("ready")
```

Brokers: `md_mq` (market data), `order_mq` (orders + positions), `oms_flags_mq` (readiness), `oms_sink_mq` (gateway / OMS / fills).

> [!IMPORTANT]
> Symbol keys on Kafka must match `BINANCE_SPOT:BTCUSDT` style (`aoao::core::Symbol::key()`). A typo in `STRAT_SYMBOLS` looks like a dead strategy with zero logs.

> [!TIP]
> Run the full stack from `**shared**`: `docker compose up --build -d`. Each service repo has its own README for build, tests, and env vars.

## Services

### API message reader and normalizer service (`api-message-reader`)

Connects to crypto exchange WebSockets and reads streaming market data (book diffs and trades). Each exchange uses a different payload shape, so this service implements per-exchange normalization and publishes a single internal format (FlatBuffers on Kafka). Deploy one reader instance per symbol or currency pair; in compose that is `md_reader_xrp`, `md_reader_btc`, and so on. It does not keep a full order book; it only forwards normalized events.

### Order Book Advisor service (`order-book-advisor`)

Serves the current limit order book snapshot over HTTP. When a strategy starts, it can call the advisor to seed its local book before applying live diffs. On a cache miss the advisor fetches depth from the exchange REST API, normalizes it, and stores JSON in an in-memory database (Redis). Each `(exchange, symbol)` book state is cached separately so strategies get a consistent `last_update_id` to align with the stream.

### Strategy service (`strategy`)

One process per running strategy name. It consumes normalized market data from Kafka, runs decision logic, applies risk checks, and publishes orders. The modules below live inside this binary; they are separate responsibilities even though they communicate in-process.

**Local order book.** Each strategy instance owns its own books per symbol. At startup the book can be initialized from the Order Book Advisor snapshot. After that it applies stream updates from the API message reader (via `md_mq`) and tracks L1 and last trade locally.

**Strategy logic.** On each relevant market event the strategy runs signal extraction. A strategy implementation combines what it sees (limit order book, trades, and optionally statistics from the data lake) and returns buy, sell, or no action. Kinds shipped today include mean revert, momentum, passive maker, and a test cycler for pipeline checks.

**Risk engine.** Uses the local order book, current position from the portfolio feedback channel, and configured limits to approve or block an order before it leaves the process. Oversized orders and exposure breaches are rejected here so bad signals never reach the gateway.

### Execution gateway (`order_gateway`)

Runs after the risk engine inside the strategy has already decided to trade. It consumes normalized order messages from Kafka, formats them for the target exchange (Binance spot today), and sends them out. It also forwards events into the OMS sink so the order management system can track wished and active orders. When the exchange or user stream reports acceptance or rejection, downstream OMS logic updates wished vs active state accordingly.

### Order status stream handler (`order_status_stream_handler`)

Subscribes to the exchange user-data WebSocket (executions, fills, account updates). It serializes execution reports into the OMS sink topic and publishes a `**ready`** flag on `oms_flags_mq` once the stream is connected so strategies know they may send orders safely.

### Order management system (`order_management_system`)

Central in-memory store for wished and active orders (RustLite). It reads the gateway and status-handler sink, checks whether a strategy-generated order is acceptable, correlates fills, and emits execution events toward the portfolio engine. Order decline or validation failure can surface to monitoring components.

### Portfolio engine (`portfolio_engine`)

Consumes fill events from the OMS path, maintains per-strategy per-symbol position state (quantity, average price, realized PnL, commission), and publishes position updates back to Kafka so strategies and monitors stay in sync with what actually traded.

### Database service (`database`)

Parallel analytics path. Ingests trades and klines from Kafka, can poll the Order Book Advisor for L1, rolls hourly partitions into Parquet on object storage, and writes daily statistics to Postgres. Strategies can read historical stats from this data lake; the live trading loop does not depend on it.

### Monitoring and UI

**Portfolio monitor (`portfolio_monitor`).** Subscribes to position updates, persists snapshots in Postgres, and pushes live state over WebSocket for dashboards.

**Trading monitor (`trading_monitor`).** Broader desk view: wished orders, execution events, balances, mark prices, merged from Kafka and exchange streams. Can disable or flag strategies when risk rules fire (for example max loss).

**Front-end (`front-end`).** Gradio UI over portfolio and monitor endpoints so operators see open positions, PnL, and history.

**Authentication (`authentication`).** FastAPI login and token validation for gated access to the UI or other HTTP entry points.

### Shared platform (`shared`)

Docker Compose for the full pipeline, `.env` topic names, per-symbol md reader TOMLs, FlatBuffer models, and the `aoao` C++ library (`Symbol`, Kafka helpers, envelope decoding).

## Repositories


| Repo                                                                                       | Stack               |
| ------------------------------------------------------------------------------------------ | ------------------- |
| [shared](https://github.com/AOAO-Trading/shared)                                           | Docker, FlatBuffers |
| [api-message-reader](https://github.com/AOAO-Trading/api-message-reader)                   | C++                 |
| [strategy](https://github.com/AOAO-Trading/strategy)                                       | C++                 |
| [order-book-advisor](https://github.com/AOAO-Trading/order-book-advisor)                   | C++                 |
| [order_gateway](https://github.com/AOAO-Trading/order_gateway)                             | Rust                |
| [order_management_system](https://github.com/AOAO-Trading/order_management_system)         | Rust                |
| [order_status_stream_handler](https://github.com/AOAO-Trading/order_status_stream_handler) | Rust                |
| [portfolio_engine](https://github.com/AOAO-Trading/portfolio_engine)                       | Rust                |
| [portfolio_monitor](https://github.com/AOAO-Trading/portfolio_monitor)                     | Rust                |
| [database](https://github.com/AOAO-Trading/database)                                       | Python              |
| [front-end](https://github.com/AOAO-Trading/front-end)                                     | Python              |
| [authentication](https://github.com/AOAO-Trading/authentication)                           | Python              |
| [trading_monitor](https://github.com/AOAO-Trading/trading_monitor)                         | Rust                |


