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

/// A stream of a consumer connection: a slot index plus a generation.
///
/// Inert, so it can be stored anywhere. It never resolves to a slot that was reused for another stream.
public struct NIOHTTP2StreamHandle: Hashable, Sendable {
    internal var slot: UInt32
    internal var generation: UInt32

    internal init(slot: Int, generation: UInt32) {
        self.slot = UInt32(truncatingIfNeeded: slot)
        self.generation = generation
    }
}

/// A part of a stream's inbound message.
public enum NIOHTTP2InboundPart: Sendable {
    /// A header block: the initial headers, or trailers.
    case headers(HPACKHeaders, endStream: Bool)
    /// Body bytes.
    case data(ByteBuffer, endStream: Bool)
}

/// What happened to a stream since its last visit.
public struct NIOHTTP2StreamEvents: OptionSet, Hashable, Sendable {
    public var rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// The stream was opened: by the peer (servers), or by ``NIOHTTP2StreamConnection/openStream(headers:endStream:makeState:)``.
    public static var opened: Self { Self(rawValue: 1 << 0) }
    /// Inbound parts arrived.
    public static var readable: Self { Self(rawValue: 1 << 1) }
    /// The stream's buffered outbound bytes dropped below the low watermark.
    public static var writable: Self { Self(rawValue: 1 << 2) }
    /// The local END_STREAM was encoded and handed to the channel in a flush. Fires once. It never fires for a dropped
    /// or rejected frame: that stream closes with a ``NIOHTTP2Stream/closeReason`` instead.
    public static var localEndSent: Self { Self(rawValue: 1 << 3) }
    /// The stream's final visit. Its state is dropped once the visit returns.
    public static var closed: Self { Self(rawValue: 1 << 4) }
}

/// What happened to the connection since the last visit.
public struct NIOHTTP2ConnectionEvents: OptionSet, Hashable, Sendable {
    public var rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// The peer's SETTINGS changed, for example MAX_CONCURRENT_STREAMS.
    public static var settingsChanged: Self { Self(rawValue: 1 << 0) }
    /// The peer sent GOAWAY. Streams above its last stream ID close with `.refused`.
    public static var goAwayReceived: Self { Self(rawValue: 1 << 1) }
    /// Clients: ``NIOHTTP2StreamConnection/openableStreamCount`` went from 0 to more.
    public static var streamsOpenable: Self { Self(rawValue: 1 << 2) }
    /// The connection closed. Every remaining stream is in this visit with `.closed`.
    public static var closed: Self { Self(rawValue: 1 << 3) }
}

/// Why a stream closed.
public enum NIOHTTP2StreamCloseReason: Sendable {
    /// Both sides ended the stream.
    case ended
    /// RST_STREAM, sent or received.
    case reset(HTTP2ErrorCode, byPeer: Bool)
    /// The peer's GOAWAY refused the stream before it processed it: safe to retry elsewhere.
    case refused(lastStreamID: HTTP2StreamID)
    /// The connection closed or failed.
    case connectionClosed(underlying: (any Error)?)
}

/// Errors from opening and writing consumer streams.
public enum NIOHTTP2StreamError: Error {
    /// The connection is closed, or not active yet.
    case connectionClosed(underlying: (any Error)?)
    /// The peer sent GOAWAY: no new streams.
    case goAwayReceived(lastStreamID: HTTP2StreamID)
    /// Every stream ID of this connection has been used.
    case streamIDsExhausted
    /// The peer's MAX_CONCURRENT_STREAMS is reached.
    case concurrencyLimitReached(limit: Int)
    /// A write after the local end, or on a closed stream.
    case writeOutOfOrder
}

extension NIOHTTP2Handler {
    /// Which per-stream events a consumer handler also fires down the pipeline.
    public struct StreamConsumerPipelineEvents: Hashable, Sendable {
        internal var fireStreamLifecycle: Bool

        /// Stream frames and stream events reach only the consumer.
        public static var none: Self { Self(fireStreamLifecycle: false) }

        /// `NIOHTTP2StreamCreatedEvent` and `StreamClosedEvent` as well, for handlers that count streams. Costs a
        /// pipeline hop and a cast for each, per stream.
        public static var streamLifecycle: Self { Self(fireStreamLifecycle: true) }
    }
}
