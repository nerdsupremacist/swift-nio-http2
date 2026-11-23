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

/// Holds the consumer and its per-stream states for one connection.
///
/// Cycles: the consumer usually holds its `NIOHTTP2StreamConnection`, whose box holds this host. `detach()` (from
/// `handlerRemoved`) clears the box, and the core drops its reference to the host.
@usableFromInline
internal final class StreamConsumerHost<Consumer: NIOHTTP2StreamConsumer & ~Copyable> {
    @usableFromInline
    internal let core: StreamConsumerCore
    internal let box: StreamConnectionBox<Consumer>
    internal var consumer: Consumer
    @usableFromInline
    internal let states = StreamStatePages<Consumer.StreamState>()
    private let makeState: ((inout Consumer, inout NIOHTTP2Stream) -> Consumer.StreamState)?

    internal init(
        core: StreamConsumerCore,
        box: StreamConnectionBox<Consumer>,
        consumer: consuming Consumer,
        makeState: ((inout Consumer, inout NIOHTTP2Stream) -> Consumer.StreamState)?
    ) {
        self.core = core
        self.box = box
        self.consumer = consumer
        self.makeState = makeState
    }

    internal func visit() {
        let events = self.core.connectionEvents
        self.core.connectionEvents = []
        var streams = NIOHTTP2Streams<Consumer>(host: self, connectionEvents: events)
        self.consumer.processStreams(&streams)
    }

    internal func makePeerStreamState(slot: Int) {
        guard let makeState = self.makeState else { return }
        // An access like any other, so re-entering this stream from `makeStreamState` traps instead of finding no state.
        self.core.beginAccess(slot)
        var stream = NIOHTTP2Stream(core: self.core, slot: slot, events: [.opened])
        let state = makeState(&self.consumer, &stream)
        _ = self.core.endAccess(slot, presented: [])
        self.states.pointer(at: slot).pointee = state
    }

    internal func detach() {
        self.box.host = nil
    }

    /// The core's only way into the typed consumer: one call per visit, plus one per stream the peer opens.
    internal var calls: StreamConsumerCore.HostCalls {
        StreamConsumerCore.HostCalls(
            visit: { self.visit() },
            makePeerStreamState: { self.makePeerStreamState(slot: $0) },
            detach: { self.detach() }
        )
    }

    /// Lends a stream and its state for one access, inside a batch.
    @inlinable
    internal func access<Result, Failure: Error>(
        slot: Int,
        events: NIOHTTP2StreamEvents,
        _ body: (inout NIOHTTP2Stream, inout Consumer.StreamState) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        self.core.beginAccess(slot)
        let pointer = self.states.pointer(at: slot)
        var stream = NIOHTTP2Stream(core: self.core, slot: slot, events: events)
        let result: Result
        do {
            // The pointer stays valid during the access: pages never move, and the slot can't be freed while accessed.
            result = try body(&stream, &pointer.pointee!)
        } catch {
            _ = self.core.endAccess(slot, presented: [])
            throw error
        }
        if self.core.endAccess(slot, presented: events) {
            pointer.pointee = nil
        }
        return result
    }
}

/// The connection's handle on its host. Cleared when the handler is removed.
@usableFromInline
internal final class StreamConnectionBox<Consumer: NIOHTTP2StreamConsumer & ~Copyable> {
    @usableFromInline
    internal var host: StreamConsumerHost<Consumer>?
    internal var eventLoop: (any EventLoop)?

    internal init() {}
}

@available(*, unavailable)
extension StreamConsumerHost: Sendable {}

@available(*, unavailable)
extension StreamConnectionBox: Sendable {}
