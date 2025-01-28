# LeDron-James

**LeDron-James** is a Rust-based simulation library that models drone behavior in a network protocol scenario. 
The library simulates drones that handle various types of network packets, including acknowledgment (Ack), negative acknowledgment (Nack), flood requests, and fragments. 
The drones operate under a network where they process packets, perform routing, and handle events triggered by a simulation controller. 

### Key Features:
- **Packet Handling**: The drones process different packet types such as `Ack`, `Nack`, `FloodRequest`, `MsgFragment`, and `FloodResponse`.
- **Routing and Flooding**: Supports a routing mechanism with hops and handles packet flooding requests across neighboring drones.
- **Crash and Recovery**: Simulate drone crashes and recovery, influencing packet forwarding behavior.
- **Simulation Control**: Provides a controller interface to manipulate drone behavior, such as setting packet drop rates or crashing the drone for testing network resilience.

## Installation

Add this dependency to your `Cargo.toml`:

```toml
[dependencies]
LeDron_James = { git = "https://github.com/anass03/LeDron_James.git", features = ["log"] }

[features]
log = []
default = ["log"]
```
## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.
