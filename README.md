# Securities Information Processor

Captures trade's for a selected group of symbols, or all symbols using `*`.

## Requirements

- [QuestDB](https://questdb.io/docs/) - A time series database that is very fast.

## Installation

```bash
go install github.com/phoobynet/sip-observer
```

Create `.toml` file, and decide what symbols you would like to include.  

- `title` - Whatever you want it to be.
- `symbols` - e.g. `"AAPL"`, etc. Note that `*` means everything (really hope your hardware is up to it).
- `db_host` - The host address and port of QuestDB

### Example configuration file
```toml
title = "Everything"

symbols = [
    "*",
]

db_host = "192.168.1.66:9009"
```

### Run

```bash
sip-observer --config config.toml
```


