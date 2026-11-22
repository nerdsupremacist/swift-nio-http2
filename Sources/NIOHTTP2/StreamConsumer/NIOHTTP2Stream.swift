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

/// One stream, for one access. Lent `inout` only.
///
/// Writes are held until the next write or the end of the access, so END_STREAM can fold into the last frame. They
/// leave in one flush at the end of the outermost access.
public struct NIOHTTP2Stream: ~Copyable {
    // Valid for the access: the core outlives every access it lends.
    internal unowned(unsafe) let core: StreamConsumerCore
    internal let slot: Int

    /// The events presented to this visit. Empty outside a visit.
    public let events: NIOHTTP2StreamEvents

    internal init(core: StreamConsumerCore, slot: Int, events: NIOHTTP2StreamEvents) {
        self.core = core
        self.slot = slot
        self.events = events
    }

    /// This stream's handle, to reach it later with `withStream`.
    public var handle: NIOHTTP2StreamHandle {
        self.core.handle(for: self.slot)
    }

    /// The stream's ID.
    public var id: HTTP2StreamID {
        self.core.records[self.slot].id
    }

    /// Why the stream closed, once it has.
    public var closeReason: NIOHTTP2StreamCloseReason? {
        self.core.records[self.slot].closeReason
    }

    /// The next inbound part, or `nil`.
    ///
    /// Taking DATA credits its bytes to the flow-control windows: untaken parts are the back pressure. Parts left
    /// untaken don't bring the stream back to a visit; take them later with `withStream`.
    public mutating func nextInboundPart() -> NIOHTTP2InboundPart? {
        guard let part = self.core.records[self.slot].inbound.popFirst() else { return nil }
        if case .data(let buffer, _) = part,
            let increment = self.core.records[self.slot].window.bufferedFrameEmitted(size: buffer.readableBytes)
        {
            self.core.writeWindowUpdate(self.core.records[self.slot].id, increment: increment)
        }
        return part
    }

    /// Whether the peer ended the stream and every part was taken.
    public var isInboundComplete: Bool {
        self.core.records[self.slot].inboundComplete && self.core.records[self.slot].inbound.isEmpty
    }

    /// Advisory: the bytes that fit below the high watermark while the channel is writable. Writing more is accepted
    /// and waits inside nio-http2; `.writable` fires once the buffered bytes drop below the low watermark.
    public var writableBytes: Int {
        self.core.writableBytes(self.slot)
    }

    /// Writes a header block (the initial headers on a server, or informational responses).
    public mutating func writeHeaders(_ headers: HPACKHeaders) throws(NIOHTTP2StreamError) {
        try self.core.writeHeaders(self.slot, headers)
    }

    /// Writes body bytes.
    public mutating func writeData(_ data: ByteBuffer) throws(NIOHTTP2StreamError) {
        try self.core.writeData(self.slot, .byteBuffer(data))
    }

    /// Writes body bytes or a file region (sent without copying, as with a stream channel).
    public mutating func writeData(_ data: IOData) throws(NIOHTTP2StreamError) {
        try self.core.writeData(self.slot, data)
    }

    /// Ends the local side: folds END_STREAM into the last held frame, or sends trailers, or an empty DATA frame.
    public mutating func writeEnd(trailers: HPACKHeaders? = nil) throws(NIOHTTP2StreamError) {
        try self.core.writeEnd(self.slot, trailers: trailers)
    }

    /// Whether the local side has ended.
    public var isOutboundComplete: Bool {
        self.core.records[self.slot].localEndWritten
    }

    /// Opt-in socket-level completion: succeeds once the stream's latest write (or its end) is written, fails if it's
    /// dropped. No promise exists unless one is asked for.
    public mutating func completeWhenWritten(_ promise: EventLoopPromise<Void>) {
        self.core.completeWhenWritten(self.slot, promise)
    }

    /// Sends RST_STREAM. Unsent writes are dropped. Does nothing once the stream is closed.
    public mutating func reset(_ code: HTTP2ErrorCode) {
        self.core.resetStream(self.slot, code: code)
    }
}

@available(*, unavailable)
extension NIOHTTP2Stream: Sendable {}

/// The streams of a visit or an out-of-band access. Lent `inout` only.
public struct NIOHTTP2Streams<Consumer: NIOHTTP2StreamConsumer & ~Copyable>: ~Copyable {
    // Strong: an `unowned(unsafe)` reference to a generic class with a noncopyable argument crashes IRGen (Swift 6.4).
    internal let host: StreamConsumerHost<Consumer>

    /// The connection's events since the last visit. Empty outside a visit.
    public let connectionEvents: NIOHTTP2ConnectionEvents

    internal init(host: StreamConsumerHost<Consumer>, connectionEvents: NIOHTTP2ConnectionEvents) {
        self.host = host
        self.connectionEvents = connectionEvents
    }

    /// The peer's MAX_CONCURRENT_STREAMS as nio-http2 applies it.
    public var remoteMaxConcurrentStreams: Int? {
        self.host.core.handler?.streamConsumerMaxOutboundStreams
    }

    /// The last stream ID of the peer's GOAWAY, if one arrived.
    public var goAwayLastStreamID: HTTP2StreamID? {
        self.host.core.goAwayLastStreamID
    }

    /// The streams that have state.
    public var count: Int {
        self.host.core.occupiedCount
    }

    /// Visits the streams with events, oldest first. A visit whose events contain `.closed` is the stream's last: its
    /// state is dropped once `body` returns. If `body` throws, the stream it threw on stays ready for the next visit.
    public mutating func forEachReady<Failure: Error>(
        _ body: (_ stream: inout NIOHTTP2Stream, _ state: inout Consumer.StreamState) throws(Failure) -> Void
    ) throws(Failure) {
        let core = self.host.core
        while let (slot, events) = core.dequeueReady() {
            do {
                try self.host.access(slot: slot, events: events, body)
            } catch {
                core.requeueFront(slot, events: events)
                throw error
            }
        }
    }

    /// Runs `body` on one stream. Re-entering a stream that is already being accessed traps, like an exclusivity
    /// violation. Returns `nil` for a stale handle.
    public mutating func withStream<Result, Failure: Error>(
        _ handle: NIOHTTP2StreamHandle,
        _ body: (_ stream: inout NIOHTTP2Stream, _ state: inout Consumer.StreamState) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        guard let slot = self.host.core.slot(for: handle) else { return nil }
        return try self.host.access(slot: slot, events: [], body)
    }
}

@available(*, unavailable)
extension NIOHTTP2Streams: Sendable {}
