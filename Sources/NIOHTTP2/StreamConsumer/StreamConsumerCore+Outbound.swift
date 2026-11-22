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

// Outbound: writes, END folding, watermarks and the handler's encode and flush hooks.
extension StreamConsumerCore {
    internal func write(_ frame: HTTP2Frame, promise: EventLoopPromise<Void>?) {
        guard let handler = self.handler, let context = self.context else {
            promise?.fail(NIOHTTP2StreamError.connectionClosed(underlying: self.connectionError))
            return
        }
        self.needsFlush = true
        handler.writeFromStreamConsumer(context: context, frame: frame, promise: promise)
    }

    private func canWrite(_ slot: Int) -> Bool {
        self.records[slot].closeReason == nil && !self.records[slot].localEndWritten
    }

    internal func writeHeaders(_ slot: Int, _ headers: HPACKHeaders) throws(NIOHTTP2StreamError) {
        guard self.canWrite(slot) else { throw .writeOutOfOrder }
        self.releaseHeldWrite(slot)
        self.records[slot].held = HeldWrite(
            payload: .headers(.init(headers: headers)),
            promise: self.takeCompletion(slot)
        )
    }

    internal func writeData(_ slot: Int, _ data: IOData) throws(NIOHTTP2StreamError) {
        guard self.canWrite(slot) else { throw .writeOutOfOrder }
        self.releaseHeldWrite(slot)
        self.records[slot].pendingBytes += data.readableBytes
        if self.records[slot].pendingBytes > self.highWatermark {
            self.records[slot].isAboveHighWatermark = true
        }
        self.records[slot].held = HeldWrite(
            payload: .data(.init(data: data)),
            promise: self.takeCompletion(slot)
        )
    }

    /// Folds END_STREAM into the held frame, or sends trailers, or an empty DATA frame.
    internal func writeEnd(_ slot: Int, trailers: HPACKHeaders?) throws(NIOHTTP2StreamError) {
        guard self.canWrite(slot) else { throw .writeOutOfOrder }
        self.records[slot].localEndWritten = true
        let id = self.records[slot].id
        if let trailers {
            self.releaseHeldWrite(slot)
            self.write(
                HTTP2Frame(streamID: id, payload: .headers(.init(headers: trailers, endStream: true))),
                promise: self.takeCompletion(slot)
            )
            return
        }
        guard var held = self.records[slot].held else {
            self.write(
                HTTP2Frame(streamID: id, payload: .data(.init(data: .byteBuffer(ByteBuffer()), endStream: true))),
                promise: self.takeCompletion(slot)
            )
            return
        }
        self.records[slot].held = nil
        switch held.payload {
        case .headers(var headers):
            headers.endStream = true
            held.payload = .headers(headers)
        case .data(var data):
            data.endStream = true
            held.payload = .data(data)
        default:
            preconditionFailure("Only HEADERS and DATA are held")
        }
        held.promise.setOrCascade(to: self.takeCompletion(slot))
        self.write(HTTP2Frame(streamID: id, payload: held.payload), promise: held.promise)
    }

    internal func releaseHeldWrite(_ slot: Int) {
        guard let held = self.records[slot].held else { return }
        self.records[slot].held = nil
        self.write(HTTP2Frame(streamID: self.records[slot].id, payload: held.payload), promise: held.promise)
    }

    internal func completeWhenWritten(_ slot: Int, _ promise: EventLoopPromise<Void>) {
        if self.records[slot].closeReason != nil {
            promise.fail(NIOHTTP2StreamError.connectionClosed(underlying: nil))
        } else if self.records[slot].held != nil {
            self.records[slot].held!.promise.setOrCascade(to: promise)
        } else if self.records[slot].localEndWritten {
            // The end is out of our hands already: complete with `.localEndSent`.
            self.records[slot].completion.setOrCascade(to: promise)
        } else {
            // Attached to the next frame this stream writes.
            self.records[slot].completion.setOrCascade(to: promise)
        }
    }

    private func takeCompletion(_ slot: Int) -> EventLoopPromise<Void>? {
        let completion = self.records[slot].completion
        self.records[slot].completion = nil
        return completion
    }

    internal func resetStream(_ slot: Int, code: HTTP2ErrorCode) {
        guard self.records[slot].closeReason == nil else { return }
        if let held = self.records[slot].held {
            self.records[slot].held = nil
            held.promise?.fail(NIOHTTP2StreamError.writeOutOfOrder)
        }
        self.records[slot].localReset = true
        let wasCreated = self.records[slot].isCreated || !self.records[slot].isLocal
        self.write(HTTP2Frame(streamID: self.records[slot].id, payload: .rstStream(code)), promise: nil)
        if !wasCreated {
            // HEADERS still buffered inside nio-http2: the RST dropped them, and no close event follows.
            self.close(slot, reason: .reset(code, byPeer: false))
        }
    }

    internal func writableBytes(_ slot: Int) -> Int {
        guard self.parentIsWritable, self.canWrite(slot) else { return 0 }
        return max(0, self.highWatermark - self.records[slot].pendingBytes)
    }

    /// The handler encoded an outbound frame: its bytes leave the watermarks, and a local END is recorded until the flush.
    internal func outboundEncoded(_ frame: HTTP2Frame) {
        guard frame.streamID != .rootStream, let slot = self.slotByID[frame.streamID] else { return }
        let endStream: Bool
        switch frame.payload {
        case .data(let data):
            endStream = data.endStream
            self.records[slot].pendingBytes -= data.data.readableBytes
            if self.records[slot].isAboveHighWatermark, self.records[slot].pendingBytes <= self.lowWatermark {
                self.records[slot].isAboveHighWatermark = false
                self.markReady(slot, .writable)
            }
        case .headers(let headers):
            endStream = headers.endStream
        default:
            return
        }
        if endStream, self.records[slot].localEndWritten, !self.records[slot].localEndEncoded {
            self.records[slot].localEndEncoded = true
            self.encodedEnds.append(slot)
        }
    }

    /// The handler dropped or failed one of a stream's frames before it reached the wire. The stream won't make progress
    /// (a failed HEADERS leaves it open inside nio-http2), so its next visit is its last. Frames dropped by the consumer's
    /// own reset keep the reset as the reason.
    internal func outboundFailed(_ frame: HTTP2Frame, error: any Error) {
        guard frame.streamID != .rootStream, let slot = self.slotByID[frame.streamID],
            !self.records[slot].localReset
        else { return }
        self.close(slot, reason: .writeFailed(underlying: error))
    }

    /// The handler flushed: every END encoded before it is now `.localEndSent`.
    internal func didFlush() {
        guard !self.encodedEnds.isEmpty else { return }
        for slot in self.encodedEnds where self.records[slot].isOccupied && self.records[slot].localEndEncoded {
            if let completion = self.records[slot].completion {
                self.records[slot].completion = nil
                completion.succeed()
            }
            self.markReady(slot, .localEndSent)
        }
        self.encodedEnds.removeAll(keepingCapacity: true)
        if !self.isFlushing {
            self.visitIfNeeded()
        }
    }
}

// Client streams, capacity and GOAWAY.
extension StreamConsumerCore {
    internal var maximumStreamID: Int { Int(Int32.max) }

    internal var openableStreamCount: Int {
        guard self.isActive, !self.isClosed, self.goAwayLastStreamID == nil, let handler = self.handler else {
            return 0
        }
        guard self.nextStreamID <= self.maximumStreamID else { return 0 }
        let idsLeft = (self.maximumStreamID - self.nextStreamID) / 2 + 1
        return max(0, min(handler.streamConsumerMaxOutboundStreams - self.openLocalStreams, idsLeft))
    }

    internal func checkOpenable() {
        guard self.isClient else { return }
        let count = self.openableStreamCount
        if count > 0, self.lastOpenableCount == 0 {
            self.connectionEvents.insert(.streamsOpenable)
        }
        self.lastOpenableCount = count
    }

    /// Assigns the next ID and writes HEADERS. The caller makes the state and runs this inside a batch.
@usableFromInline
    internal func openLocalStream(headers: HPACKHeaders, endStream: Bool) throws(NIOHTTP2StreamError) -> Int {
        guard self.isActive, !self.isClosed, let handler = self.handler else {
            throw .connectionClosed(underlying: self.connectionError)
        }
        if let lastStreamID = self.goAwayLastStreamID {
            throw .goAwayReceived(lastStreamID: lastStreamID)
        }
        guard self.nextStreamID <= self.maximumStreamID else { throw .streamIDsExhausted }
        let limit = handler.streamConsumerMaxOutboundStreams
        guard self.openLocalStreams < limit else { throw .concurrencyLimitReached(limit: limit) }

        let id = HTTP2StreamID(self.nextStreamID)
        self.nextStreamID += 2
        self.openLocalStreams += 1
        self.lastOpenableCount = self.openableStreamCount
        let slot = self.allocateSlot(id: id, isLocal: true)
        self.records[slot].events = [.opened]
        if endStream {
            self.records[slot].localEndWritten = true
        }
        self.write(
            HTTP2Frame(streamID: id, payload: .headers(.init(headers: headers, endStream: endStream))),
            promise: nil
        )
        return slot
    }

    /// Streams above the GOAWAY's last stream ID whose HEADERS nio-http2 still buffers (it never created them) are
    /// purged with RST_STREAM, which drops the buffered frames without writing anything, and closed as refused.
    internal func sweepAboveGoAway() {
        guard let lastStreamID = self.goAwayLastStreamID else { return }
        for slot in self.records.indices {
            let record = self.records[slot]
            guard record.isOccupied, record.isLocal, !record.isCreated, record.closeReason == nil,
                record.id > lastStreamID
            else { continue }
            self.records[slot].localReset = true
            self.write(HTTP2Frame(streamID: record.id, payload: .rstStream(.refusedStream)), promise: nil)
            self.close(slot, reason: .refused(lastStreamID: lastStreamID))
        }
    }
}
