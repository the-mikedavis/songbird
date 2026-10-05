# Songbird

This is a Rust client for the RabbitMQ streams protocol. It was built from scratch for learning purposes.

The code is meant to be readable and idiomatic Rust. It's Tokio-flavored, so it relies on `bytes` for serde and uses `async fn` for almost all I/O interaction. Songbird wraps the API as-is without conveniences (for example client-side batching).

## Status/Maintenance and License

This is a minimal library that I wrote to help me understand the streams protocol. It is not intended for production usage; please don't use it that way. https://github.com/rabbitmq/rabbitmq-stream-rust-client is the right way to use the streams protocol from Rust (as of time of writing at least).

Since this work is meant only for education purposes, it is licensed under BSD-0 - a no-attribution license. Instead of using it directly, feel free to copy, paste, and adapt it into your own library. For example you might extend the library to add a batching producer that accepts `N` publishes or `T` delay before sending a compressed batch, which is a Tokio task.
