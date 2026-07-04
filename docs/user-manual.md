# Georuggine — User manual

Trip-tracking app with a live map of Turin and chat. One server, multiple clients (CLI or browser).

## Requirements

- Rust + Cargo (build from source)
- Map only: `wasm-pack` + `wasm32-unknown-unknown` target (one-time)

## Installation

```bash
cargo build --release -p server -p client
```

Binaries in `target/release/` (`server`, `client`).

## Map (one-time)

The page shows a vehicle moving on a map of Turin. Two artifacts are needed, generated once:

1. Compile the simulator to WebAssembly:

   ```bash
   rustup target add wasm32-unknown-unknown
   cargo install wasm-pack
   wasm-pack build sim --release --target web --out-dir ../web/pkg
   ```

2. Generate the road graph (Python 3, stdlib only; downloads from Overpass):

   ```bash
   python tools/export_turin_graph.py     # -> web/data/graph.json
   ```

The graph is already versioned in the repo; the WASM artifacts are not — regenerate them with the command above after every change to the `sim` crate.

## Startup

```bash
cargo run -p server     # terminal 1
cargo run -p client     # terminal 2
```

Server on `http://127.0.0.1:7878`. Open that URL in the browser for the map.

## Usage

**CLI client** — commands:

```
/register <user> <password>
/login <user> <password>
/start                 start a trip
/end                   end the trip
/chat <text>           broadcast  (@user text = private message)
/quit
```

**Browser** — Register / Login / Start trip / End trip / Chat / Disconnect buttons. Open multiple tabs to simulate multiple users.

## LAN usage

To connect clients from other devices on the same network.

1. Start the server bound to all interfaces:

   ```powershell
   $env:SERVER_ADDR="0.0.0.0:7878"; cargo run -p server     # Linux/macOS: SERVER_ADDR=0.0.0.0:7878 cargo run -p server
   ```

2. Find the server's IP:

   ```
   ipconfig
   ```

   Take the IPv4 of the Wi-Fi/Ethernet network (`192.168.x.x` or `10.x.x.x`).

3. From the other devices open `http://<server-IP>:7878` in the browser.
