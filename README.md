# Securities Information Processor

Captures trade's for a selected group of symbols, or all symbols using `*`.

## Requirements

- [QuestDB](https://questdb.io/docs/) - A time series database that is very fast.

## Installation

Set the following environment variables

- `APCA_API_KEY_ID` - Your Alpaca Key
- `APCA_API_SECRET_KEY` - Your Alpaca Secret
- `APCA_API_BASE_URL` - For this application, set it to https://paper-api.alpaca.markets (considering not making this a requirement)

Install the packages

```bash
go install github.com/phoobynet/sip-observer@latest
```

Create `.toml` file, and decide what symbols you would like to include.  

- `title` - Whatever you want it to be.
- `symbols` - e.g. `"AAPL"`, etc. Note that `*` means everything (really hope your hardware is up to it).
- `db_host` - The host address and port of QuestDB

**Example `config.toml`**

```toml
title = "Everything"

symbols = [
    "*",
]

db_host = "localhost:9009"
```

Assuming you have a QuestDB server up and running, start `sip-observer`

```bash
sip-observer --config config.toml
```


