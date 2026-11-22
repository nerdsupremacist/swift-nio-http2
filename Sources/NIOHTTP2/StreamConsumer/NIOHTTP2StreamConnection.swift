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

import NIOCore
import NIOHPACK

/// The consumer's way into its connection outside a visit.
///
/// Event loop only: to use it elsewhere, hop with `NIOLoopBound`. It stays safe after the handler is removed: every
/// method then returns `nil` or throws ``NIOHTTP2StreamError/connectionClosed(underlying:)``.
public struct NIOHTTP2StreamConnection<Consumer: NIOHTTP2StreamConsumer & ~Copyable> {
    @usableFromInline
    internal let box: StreamConnectionBox<Consumer>

    @usableFromInline
    internal init(box: StreamConnectionBox<Consumer>) {
        self.box = box
    }

    /// The connection's event loop.
    public var eventLoop: any EventLoop {
        self.box.eventLoop!
    }

    /// Runs `body` with the streams. Inside a visit or a read batch it joins that batch; otherwise one flush follows,
    /// and a visit if streams have events afterwards. Returns `nil` once closed.
    @inlinable
    public func withStreams<Result, Failure: Error>(
        _ body: (_ streams: inout NIOHTTP2Streams<Consumer>) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        guard let host = self.box.host else { return nil }
        return try host.core.withBatch { () throws(Failure) -> Result in
            var streams = NIOHTTP2Streams<Consumer>(host: host, connectionEvents: [])
            return try body(&streams)
        }
    }

    /// Runs `body` on one stream, in a batch as ``withStreams(_:)``. Re-entering the stream that is already being
    /// accessed traps, like an exclusivity violation. Returns `nil` for a stale handle or once closed.
    @inlinable
    public func withStream<Result, Failure: Error>(
        _ handle: NIOHTTP2StreamHandle,
        _ body: (_ stream: inout NIOHTTP2Stream, _ state: inout Consumer.StreamState) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        guard let host = self.box.host, let slot = host.core.slot(for: handle) else { return nil }
        return try host.core.withBatch { () throws(Failure) -> Result in
            try host.access(slot: slot, events: [], body)
        }
    }

    /// Sends GOAWAY with the highest stream ID the peer opened so far.
    public func sendGoAway(errorCode: HTTP2ErrorCode) {
        guard let host = self.box.host else { return }
        host.core.withBatch {
            host.core.write(
                HTTP2Frame(
                    streamID: .rootStream,
                    payload: .goAway(lastStreamID: .maxID, errorCode: errorCode, opaqueData: nil)
                ),
                promise: nil
            )
        }
    }
}

extension NIOHTTP2StreamConnection where Consumer: NIOHTTP2ClientStreamConsumer {
    /// How many streams can be opened now: the peer's MAX_CONCURRENT_STREAMS minus the open client streams, capped by
    /// the stream IDs left. 0 before the connection is active, after GOAWAY, or once closed.
    public var openableStreamCount: Int {
        self.box.host?.core.openableStreamCount ?? 0
    }

    /// Opens a stream: assigns its ID, writes HEADERS, then runs `makeState`. Its first visit carries `.opened`.
    ///
    /// - Throws: ``NIOHTTP2StreamError``: `.concurrencyLimitReached`, `.goAwayReceived`, `.streamIDsExhausted` or
    ///   `.connectionClosed`. Nothing is written when it throws.
    @inlinable
    public func openStream(
        headers: HPACKHeaders,
        endStream: Bool,
        makeState: (_ stream: inout NIOHTTP2Stream) -> Consumer.StreamState
    ) throws(NIOHTTP2StreamError) -> NIOHTTP2StreamHandle {
        guard let host = self.box.host else { throw .connectionClosed(underlying: nil) }
        let core = host.core
        return try core.withBatch { () throws(NIOHTTP2StreamError) -> NIOHTTP2StreamHandle in
            let slot = try core.openLocalStream(headers: headers, endStream: endStream)
            let handle = core.handle(for: slot)
            var stream = NIOHTTP2Stream(core: core, slot: slot, events: [.opened])
            host.states.pointer(at: slot).pointee = makeState(&stream)
            return handle
        }
    }
}

@available(*, unavailable)
extension NIOHTTP2StreamConnection: Sendable {}
