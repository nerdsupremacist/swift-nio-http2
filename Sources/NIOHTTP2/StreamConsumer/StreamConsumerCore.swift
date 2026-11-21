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

/// The non-generic part of a consumer connection: per-stream records, the ready list, inbound windows, batching and
/// visits. It's the `NIOHTTP2Handler`'s fourth multiplexer mode, and reaches the typed consumer through one existential
/// call per visit. Event loop only.
///
/// Re-entrancy: writes go straight into the handler, which can fire stream events synchronously, so a record may close
/// while a view of it is in use. Records are only ever accessed briefly, never across a call out.
@usableFromInline
internal final class StreamConsumerCore {
    internal struct Record {
        var generation: UInt32 = 0
        var isOccupied = false
        var id = HTTP2StreamID.rootStream
        var isLocal = false
        var isCreated = false
        var isQueued = false
        var isAccessed = false
        var events: NIOHTTP2StreamEvents = []
        var closeReason: NIOHTTP2StreamCloseReason?
        var inbound = CircularBuffer<NIOHTTP2InboundPart>(initialCapacity: 2)
        var inboundComplete = false
        var window: InboundWindowManager
        var held: HeldWrite?
        var localEndWritten = false
        var localEndEncoded = false
        var localReset = false
        var pendingBytes = 0
        var isAboveHighWatermark = false
        var completion: EventLoopPromise<Void>?

        init(targetWindowSize: Int32) {
            self.window = InboundWindowManager(targetSize: targetWindowSize)
        }

        /// Empties the record for the slot's next stream, keeping the inbound buffer's storage (no allocation).
        mutating func reset(targetWindowSize: Int32) {
            self.generation &+= 1
            self.isOccupied = false
            self.id = .rootStream
            self.isLocal = false
            self.isCreated = false
            self.isQueued = false
            self.isAccessed = false
            self.events = []
            self.closeReason = nil
            self.inbound.removeAll(keepingCapacity: true)
            self.inboundComplete = false
            self.window = InboundWindowManager(targetSize: targetWindowSize)
            self.held = nil
            self.localEndWritten = false
            self.localEndEncoded = false
            self.localReset = false
            self.pendingBytes = 0
            self.isAboveHighWatermark = false
            self.completion = nil
        }
    }

    /// The typed host's entry points, as closures: forming an existential of a type with noncopyable generic
    /// arguments needs a newer runtime than nio-http2 supports.
    internal struct HostCalls {
        let visit: () -> Void
        let makePeerStreamState: (Int) -> Void
        let detach: () -> Void
    }

    internal struct HeldWrite {
        var payload: HTTP2Frame.FramePayload
        var promise: EventLoopPromise<Void>?
    }

    internal let isClient: Bool
    internal let pipelineEvents: NIOHTTP2Handler.StreamConsumerPipelineEvents
    internal let streamWindowTarget: Int32
    internal let highWatermark: Int
    internal let lowWatermark: Int

    internal var handler: NIOHTTP2Handler?
    internal var context: ChannelHandlerContext?
    internal var host: HostCalls?

    internal var records: ContiguousArray<Record> = []
    internal var freeSlots: [Int] = []
    internal var slotByID: [HTTP2StreamID: Int] = [:]
    internal var ready = CircularBuffer<Int>(initialCapacity: 16)
    internal var readyVersion = 0
    internal var encodedEnds: [Int] = []
    internal var connectionWindow: InboundWindowManager
    internal var connectionEvents: NIOHTTP2ConnectionEvents = []

    internal var batchDepth = 0
    internal var needsFlush = false
    internal var isReading = false
    internal var isVisiting = false
    internal var isFlushing = false
    internal var parentIsWritable = true

    internal var isActive = false
    internal var isClosed = false
    internal var connectionError: (any Error)?
    internal var nextStreamID: Int
    internal var openLocalStreams = 0
    internal var goAwayLastStreamID: HTTP2StreamID?
    internal var needsGoAwaySweep = false
    internal var lastOpenableCount = 0

    /// For tests: visits made so far.
    internal var visitCount = 0

    internal init(
        isClient: Bool,
        configuration: NIOHTTP2Handler.Configuration,
        pipelineEvents: NIOHTTP2Handler.StreamConsumerPipelineEvents
    ) {
        self.isClient = isClient
        self.pipelineEvents = pipelineEvents
        self.streamWindowTarget = Int32(configuration.stream.targetWindowSize.clampedToValidWindowSize)
        self.highWatermark = configuration.stream.outboundBufferSizeHighWatermark
        self.lowWatermark = configuration.stream.outboundBufferSizeLowWatermark
        self.connectionWindow = InboundWindowManager(
            targetSize: Int32(configuration.connection.targetWindowSize.clampedToValidWindowSize)
        )
        self.nextStreamID = isClient ? 1 : 2
    }

    // Slots.

    internal func allocateSlot(id: HTTP2StreamID, isLocal: Bool) -> Int {
        let slot: Int
        if let free = self.freeSlots.popLast() {
            slot = free
        } else {
            slot = self.records.count
            self.records.append(Record(targetWindowSize: self.streamWindowTarget))
        }
        self.records[slot].isOccupied = true
        self.records[slot].id = id
        self.records[slot].isLocal = isLocal
        self.slotByID[id] = slot
        return slot
    }

    /// Frees a slot after its final visit. The record keeps its buffers for the next stream.
    internal func freeSlot(_ slot: Int) {
        let id = self.records[slot].id
        if self.slotByID[id] == slot {
            self.slotByID.removeValue(forKey: id)
        }
        self.records[slot].reset(targetWindowSize: self.streamWindowTarget)
        self.freeSlots.append(slot)
    }

    internal func slot(for handle: NIOHTTP2StreamHandle) -> Int? {
        let slot = Int(handle.slot)
        guard slot < self.records.count, self.records[slot].isOccupied,
            self.records[slot].generation == handle.generation
        else {
            return nil
        }
        return slot
    }

    internal func handle(for slot: Int) -> NIOHTTP2StreamHandle {
        NIOHTTP2StreamHandle(slot: slot, generation: self.records[slot].generation)
    }

    internal var occupiedCount: Int {
        self.records.count - self.freeSlots.count
    }

    internal func markReady(_ slot: Int, _ events: NIOHTTP2StreamEvents) {
        self.records[slot].events.formUnion(events)
        self.readyVersion &+= 1
        guard !self.records[slot].isQueued else { return }
        self.records[slot].isQueued = true
        self.ready.append(slot)
    }

    // Visits and batches.

    /// Starts one stream's visit: takes its events and marks it accessed. The caller ends it with `endVisit`.
    internal func beginAccess(_ slot: Int) {
        precondition(!self.records[slot].isAccessed, "This stream is already in use.")
        self.records[slot].isAccessed = true
    }

    /// Ends an access: releases the held write. Returns true if the visit presented `.closed` and the slot is now free.
    internal func endAccess(_ slot: Int, presented events: NIOHTTP2StreamEvents) -> Bool {
        self.records[slot].isAccessed = false
        self.releaseHeldWrite(slot)
        if events.contains(.closed) {
            self.freeSlot(slot)
            return true
        }
        return false
    }

    internal func dequeueReady() -> (slot: Int, events: NIOHTTP2StreamEvents)? {
        guard let slot = self.ready.popFirst() else { return nil }
        self.records[slot].isQueued = false
        let events = self.records[slot].events
        self.records[slot].events = []
        return (slot, events)
    }

    internal func requeueFront(_ slot: Int, events: NIOHTTP2StreamEvents) {
        self.records[slot].events.formUnion(events)
        self.records[slot].isQueued = true
        self.ready.prepend(slot)
    }

    internal func withBatch<Result, Failure: Error>(_ body: () throws(Failure) -> Result) throws(Failure) -> Result {
        self.batchDepth += 1
        defer {
            self.batchDepth -= 1
            if self.batchDepth == 0 {
                self.endBatch()
            }
        }
        return try body()
    }

    private func endBatch() {
        self.flushIfNeeded()
        self.visitIfNeeded()
    }

    internal func flushIfNeeded() {
        guard self.needsFlush, !self.isReading, let handler = self.handler, let context = self.context else { return }
        self.needsFlush = false
        self.isFlushing = true
        handler.flush(context: context)
        self.isFlushing = false
    }

    /// Visits the consumer while streams or the connection have events, outside reads and batches.
    internal func visitIfNeeded() {
        guard self.batchDepth == 0, !self.isReading, !self.isVisiting, !self.isFlushing else { return }
        // Untaken parts don't re-queue a stream, so a visit only repeats for events that arrived during the last one
        // (for example `.localEndSent` after its flush). The bound guards against a consumer that skips its ready list.
        var rounds = 0
        while !self.ready.isEmpty || !self.connectionEvents.isEmpty, let host = self.host, rounds < 8 {
            let version = self.readyVersion
            rounds += 1
            self.visitCount += 1
            self.isVisiting = true
            self.batchDepth += 1
            host.visit()
            self.batchDepth -= 1
            self.isVisiting = false
            self.flushIfNeeded()
            if self.readyVersion == version && self.connectionEvents.isEmpty {
                break
            }
        }
    }

    // Lifecycle, called by the handler.

    internal func channelActive() {
        self.isActive = true
    }

    internal func readComplete() {
        self.isReading = false
        if self.needsGoAwaySweep {
            self.needsGoAwaySweep = false
            self.withBatch { self.sweepAboveGoAway() }
        } else {
            self.visitIfNeeded()
        }
    }

    internal func writabilityChanged(isWritable: Bool) {
        self.parentIsWritable = isWritable
        guard isWritable else { return }
        for slot in self.records.indices where self.records[slot].isOccupied {
            let record = self.records[slot]
            if record.closeReason == nil, !record.localEndWritten, record.pendingBytes < self.highWatermark {
                self.markReady(slot, .writable)
            }
        }
        self.visitIfNeeded()
    }

    internal func channelInactive() {
        self.isActive = false
        self.isReading = false
        self.closeAll()
        self.visitIfNeeded()
    }

    internal func handlerRemoved() {
        if !self.isClosed {
            self.isActive = false
            self.closeAll()
            self.visitIfNeeded()
        }
        self.host?.detach()
        self.host = nil
        self.handler = nil
        self.context = nil
    }

    private func closeAll() {
        guard !self.isClosed else { return }
        self.isClosed = true
        self.connectionEvents.insert(.closed)
        for slot in self.records.indices where self.records[slot].isOccupied {
            self.close(slot, reason: .connectionClosed(underlying: self.connectionError))
        }
    }

    /// Closes a stream: its next visit is its last.
    internal func close(_ slot: Int, reason: NIOHTTP2StreamCloseReason) {
        guard self.records[slot].closeReason == nil else { return }
        self.records[slot].closeReason = reason
        self.records[slot].window.closed = true
        if let held = self.records[slot].held {
            self.records[slot].held = nil
            held.promise?.fail(NIOHTTP2StreamError.writeOutOfOrder)
        }
        if let completion = self.records[slot].completion {
            self.records[slot].completion = nil
            completion.fail(NIOHTTP2StreamError.connectionClosed(underlying: nil))
        }
        let id = self.records[slot].id
        if self.slotByID[id] == slot {
            self.slotByID.removeValue(forKey: id)
        }
        if self.records[slot].isLocal {
            self.openLocalStreams -= 1
            self.checkOpenable()
        }
        self.markReady(slot, .closed)
    }
}

extension StreamConsumerCore: HTTP2InboundStreamMultiplexer {
    func receivedFrame(_ frame: HTTP2Frame) {
        self.isReading = true
        let streamID = frame.streamID
        if streamID == .rootStream {
            self.receivedRootFrame(frame)
            self.context?.fireChannelRead(NIOAny(frame))
            return
        }

        let part: NIOHTTP2InboundPart
        switch frame.payload {
        case .headers(let headers):
            part = .headers(headers.headers, endStream: headers.endStream)
        case .data(let data):
            guard case .byteBuffer(let buffer) = data.data else { return }
            part = .data(buffer, endStream: data.endStream)
        case .priority:
            // Always for the connection, as in the other modes.
            self.context?.fireChannelRead(NIOAny(frame))
            return
        case .rstStream, .windowUpdate, .settings, .ping, .goAway, .pushPromise, .alternativeService, .origin:
            // Delivered as stream events.
            return
        }

        var slot = self.slotByID[streamID]
        if slot == nil, !self.isClient, case .headers = part, streamID.mayBeInitiatedBy(.client) {
            slot = self.openPeerStream(streamID)
        }
        guard let slot, self.records[slot].closeReason == nil else { return }

        switch part {
        case .data(let buffer, let endStream):
            self.records[slot].window.bufferedFrameReceived(size: buffer.readableBytes)
            if endStream { self.records[slot].inboundComplete = true }
        case .headers(_, let endStream):
            if endStream { self.records[slot].inboundComplete = true }
        }
        self.records[slot].inbound.append(part)
        self.markReady(slot, .readable)
    }

    private func openPeerStream(_ id: HTTP2StreamID) -> Int? {
        guard let host = self.host else { return nil }
        let slot = self.allocateSlot(id: id, isLocal: false)
        self.records[slot].events = [.opened]
        host.makePeerStreamState(slot)
        return slot
    }

    private func receivedRootFrame(_ frame: HTTP2Frame) {
        switch frame.payload {
        case .settings(.settings):
            self.connectionEvents.insert(.settingsChanged)
            self.checkOpenable()
        case .goAway(let lastStreamID, _, _):
            self.goAwayLastStreamID = min(lastStreamID, self.goAwayLastStreamID ?? .maxID)
            self.connectionEvents.insert(.goAwayReceived)
            self.needsGoAwaySweep = true
        default:
            break
        }
    }

    func streamError(streamID: HTTP2StreamID, error: Error) {
        // nio-http2 rejected an outbound frame without closing the stream: reset it.
        guard let slot = self.slotByID[streamID] else { return }
        self.withBatch { self.resetStream(slot, code: .internalError) }
    }

    func streamCreated(event: NIOHTTP2StreamCreatedEvent) {
        if let slot = self.slotByID[event.streamID] {
            self.records[slot].isCreated = true
        }
        if self.pipelineEvents.fireStreamLifecycle {
            self.context?.fireUserInboundEventTriggered(event)
        }
    }

    func streamClosed(event: StreamClosedEvent) {
        if let slot = self.slotByID[event.streamID] {
            let reason: NIOHTTP2StreamCloseReason
            if let code = event.reason {
                if let lastStreamID = self.goAwayLastStreamID, event.streamID > lastStreamID, code == .cancel,
                    !self.records[slot].localReset
                {
                    reason = .refused(lastStreamID: lastStreamID)
                } else {
                    reason = .reset(code, byPeer: !self.records[slot].localReset)
                }
            } else {
                reason = .ended
            }
            self.close(slot, reason: reason)
        }
        if self.pipelineEvents.fireStreamLifecycle {
            self.context?.fireUserInboundEventTriggered(event)
        }
    }

    func streamWindowUpdated(event: NIOHTTP2WindowUpdatedEvent) {
        guard let size = event.inboundWindowSize else { return }
        if event.streamID == .rootStream {
            if let increment = self.connectionWindow.newWindowSize(size) {
                self.writeWindowUpdate(.rootStream, increment: increment)
            }
        } else if let slot = self.slotByID[event.streamID],
            let increment = self.records[slot].window.newWindowSize(size)
        {
            self.writeWindowUpdate(event.streamID, increment: increment)
        }
    }

    func initialStreamWindowChanged(event: NIOHTTP2BulkStreamWindowChangeEvent) {
        for slot in self.records.indices where self.records[slot].isOccupied {
            if let increment = self.records[slot].window.initialWindowSizeChanged(delta: event.delta) {
                self.writeWindowUpdate(self.records[slot].id, increment: increment)
            }
        }
    }

    internal func writeWindowUpdate(_ id: HTTP2StreamID, increment: Int) {
        self.withBatch {
            self.write(HTTP2Frame(streamID: id, payload: .windowUpdate(windowSizeIncrement: increment)), promise: nil)
        }
    }
}

@available(*, unavailable)
extension StreamConsumerCore: Sendable {}
