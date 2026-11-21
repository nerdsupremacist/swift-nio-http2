//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Serves the streams of one HTTP/2 connection without a `Channel` per stream.
///
/// nio-http2 keeps stream IDs, flow control and WINDOW_UPDATE, RST_STREAM, GOAWAY, SETTINGS and the DoS and glitch
/// limits. Stream frames never travel the pipeline. Root-stream frames (SETTINGS, PING, GOAWAY) and PRIORITY frames
/// still do, so connection handlers further down keep working. Event loop only.
///
/// Conform to ``NIOHTTP2ClientStreamConsumer`` or ``NIOHTTP2ServerStreamConsumer``, not to this protocol.
public protocol NIOHTTP2StreamConsumer: ~Copyable {
    /// The consumer's state for each stream, stored by the connection in its own slot and lent `inout`.
    associatedtype StreamState

    /// Handles the streams with events, and the connection's events.
    ///
    /// Called once per read batch, and again after an out-of-band access or a flush that leaves streams with events.
    /// Everything written in here leaves in one flush.
    mutating func processStreams(_ streams: inout NIOHTTP2Streams<Self>)
}

/// A client connection's consumer. Opens streams with ``NIOHTTP2StreamConnection/openStream(headers:endStream:makeState:)``.
public protocol NIOHTTP2ClientStreamConsumer: NIOHTTP2StreamConsumer, ~Copyable {}

/// A server connection's consumer.
public protocol NIOHTTP2ServerStreamConsumer: NIOHTTP2StreamConsumer, ~Copyable {
    /// Makes the state of a stream the client opened, before its first visit.
    ///
    /// To refuse the stream, call `stream.reset(.refusedStream)`: it gets one final visit with `.closed`.
    mutating func makeStreamState(for stream: inout NIOHTTP2Stream) -> StreamState
}
