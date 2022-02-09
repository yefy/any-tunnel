# any-tunnel
any-tunnel 建立在tcp，quic，srt等可靠协议之上

[dependencies]  
any-tunnel = { git = "https://github.com/yefy/any-tunnel.git", branch = "main" }

# 原理
原来通过一条连接收发的数据包，现在借助N条连接来进行收发，达到提速的效果。

# example
cargo run --example server
cargo run --example client
