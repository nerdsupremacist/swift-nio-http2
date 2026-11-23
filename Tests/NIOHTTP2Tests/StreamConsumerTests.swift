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
import NIOEmbedded
import NIOHPACK
import Testing

@testable import NIOHTTP2

/// What a test consumer saw, shared with the test.
final class ConsumerLog {
    var visits = 0
    var streamsPerVisit: [Int] = []
    var connectionEvents: NIOHTTP2ConnectionEvents = []
    var makeStateWasAccess: [Bool] = []
    var closeReasons: [NIOHTTP2StreamCloseReason?] = []
}

/// A client stream's observed parts and events.
final class ObservedStream {
    var parts: [NIOHTTP2InboundPart] = []
    var events: NIOHTTP2StreamEvents = []
    var closeReason: NIOHTTP2StreamCloseReason?

    var body: ByteBuffer {
        var body = ByteBuffer()
        for part in self.parts {
            if case .data(var data, _) = part { body.writeBuffer(&data) }
        }
        return body
    }

    var status: String? {
        for part in self.parts {
            if case .headers(let headers, _) = part { return headers.first(name: ":status") }
        }
        return nil
    }
}

struct TestClientConsumer: NIOHTTP2ClientStreamConsumer {
    typealias StreamState = ObservedStream
    let log: ConsumerLog

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<TestClientConsumer>) {
        self.log.visits += 1
        self.log.connectionEvents.formUnion(streams.connectionEvents)
        var count = 0
        streams.forEachReady { stream, observed in
            count += 1
            observed.events.formUnion(stream.events)
            while let part = stream.nextInboundPart() {
                observed.parts.append(part)
            }
            if stream.events.contains(.closed) {
                observed.closeReason = stream.closeReason
            }
        }
        self.log.streamsPerVisit.append(count)
    }
}

/// Echoes each request: `:status 200`, then the request body, then the end.
struct EchoServerConsumer: NIOHTTP2ServerStreamConsumer {
    struct StreamState {
        var respondedHead = false
    }

    let log: ConsumerLog

    mutating func makeStreamState(for stream: inout NIOHTTP2Stream) -> StreamState {
        self.log.makeStateWasAccess.append(stream.core.records[stream.slot].isAccessed)
        return StreamState()
    }

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<EchoServerConsumer>) {
        self.log.visits += 1
        var count = 0
        streams.forEachReady { stream, state in
            count += 1
            while let part = stream.nextInboundPart() {
                if !state.respondedHead {
                    state.respondedHead = true
                    try? stream.writeHeaders([":status": "200"])
                }
                switch part {
                case .headers(_, let endStream):
                    if endStream { try? stream.writeEnd() }
                case .data(let data, let endStream):
                    try? stream.writeData(data)
                    if endStream { try? stream.writeEnd() }
                }
            }
            if stream.events.contains(.closed) {
                self.log.closeReasons.append(stream.closeReason)
            }
        }
        self.log.streamsPerVisit.append(count)
    }
}

/// Records the frames that reach the pipeline after the HTTP/2 handler.
final class RootFrameRecorder: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame
    var frames: [HTTP2Frame] = []

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.frames.append(self.unwrapInboundIn(data))
    }
}

struct ConsumerPair {
    let client = EmbeddedChannel()
    let server = EmbeddedChannel()
    let clientLog = ConsumerLog()
    let serverLog = ConsumerLog()
    let connection: NIOHTTP2StreamConnection<TestClientConsumer>
    let serverConnection: NIOHTTP2StreamConnection<EchoServerConsumer>
    let recorder = RootFrameRecorder()

    init(
        serverSettings: HTTP2Settings = nioDefaultSettings,
        clientConfiguration: NIOHTTP2Handler.Configuration = .init(),
        connect: Bool = true
    ) throws {
        let clientLog = self.clientLog
        let serverLog = self.serverLog
        self.connection = try self.client.pipeline.syncOperations.configureHTTP2Connection(
            configuration: clientConfiguration
        ) { _ in
            TestClientConsumer(log: clientLog)
        }
        try self.client.pipeline.syncOperations.addHandler(self.recorder)
        var serverConfiguration = NIOHTTP2Handler.Configuration()
        serverConfiguration.connection.initialSettings = serverSettings
        self.serverConnection = try self.server.pipeline.syncOperations.configureHTTP2Connection(
            configuration: serverConfiguration
        ) { _ in
            EchoServerConsumer(log: serverLog)
        }
        if connect {
            try self.activate()
        }
    }

    func activate() throws {
        let address = try SocketAddress(ipAddress: "127.0.0.1", port: 0)
        try self.client.connect(to: address).wait()
        try self.server.connect(to: address).wait()
        try self.interact()
    }

    /// Moves bytes between the two channels until neither has anything to send.
    func interact() throws {
        var progressed = true
        while progressed {
            progressed = false
            for (from, to) in [(self.client, self.server), (self.server, self.client)] {
                while let data = try from.readOutbound(as: IOData.self) {
                    progressed = true
                    guard case .byteBuffer(let buffer) = data else { continue }
                    try to.writeInbound(buffer)
                }
            }
        }
    }

    func open(
        _ observed: ObservedStream,
        path: String = "/",
        endStream: Bool = true
    ) throws(NIOHTTP2StreamError) -> NIOHTTP2StreamHandle {
        let headers: HPACKHeaders = [":method": "POST", ":path": path, ":scheme": "https", ":authority": "localhost"]
        return try self.connection.openStream(headers: headers, endStream: endStream) { _ in observed }
    }

    /// Decodes and removes the frames the client has written so far.
    func drainClientFrames() throws -> [HTTP2Frame] {
        var decoder = HTTP2FrameDecoder(
            allocator: ByteBufferAllocator(),
            expectClientMagic: false,
            maximumSequentialContinuationFrames: 5
        )
        while let data = try self.client.readOutbound(as: IOData.self) {
            if case .byteBuffer(let buffer) = data { decoder.append(bytes: buffer) }
        }
        var frames: [HTTP2Frame] = []
        while let (frame, _) = try decoder.nextFrame() {
            frames.append(frame)
        }
        return frames
    }

    var clientCore: StreamConsumerCore {
        let handler = try! self.client.pipeline.syncOperations.handler(type: NIOHTTP2Handler.self)
        guard case .consumer(let core) = handler.inboundStreamMultiplexer else { fatalError("not a consumer handler") }
        return core
    }
}

/// Writes a stream's body in chunks whenever it has room below the high watermark, then the end.
struct ChunkWriterConsumer: NIOHTTP2ClientStreamConsumer {
    final class StreamState {
        var left: Int
        let chunk: Int
        init(left: Int, chunk: Int) {
            self.left = left
            self.chunk = chunk
        }
    }

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<ChunkWriterConsumer>) {
        streams.forEachReady { stream, state in Self.write(&stream, state) }
    }

    static func write(_ stream: inout NIOHTTP2Stream, _ state: StreamState) {
        while state.left > 0, stream.writableBytes > 0 {
            let size = min(state.chunk, state.left)
            try? stream.writeData(ByteBuffer(repeating: 0, count: size))
            state.left -= size
        }
        if state.left == 0, !stream.isOutboundComplete {
            try? stream.writeEnd()
        }
    }

    /// A connected client with no server (nio-http2 sends on the default settings) and small watermarks.
    static func connect(
        high: Int,
        low: Int
    ) throws -> (EmbeddedChannel, NIOHTTP2StreamConnection<ChunkWriterConsumer>) {
        let channel = EmbeddedChannel()
        var configuration = NIOHTTP2Handler.Configuration()
        configuration.stream.outboundBufferSizeHighWatermark = high
        configuration.stream.outboundBufferSizeLowWatermark = low
        let connection = try channel.pipeline.syncOperations.configureHTTP2Connection(configuration: configuration) {
            _ in ChunkWriterConsumer()
        }
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 0)).wait()
        return (channel, connection)
    }

    static func open(
        _ connection: NIOHTTP2StreamConnection<ChunkWriterConsumer>,
        _ state: StreamState
    ) throws -> NIOHTTP2StreamHandle {
        let headers: HPACKHeaders = [":method": "POST", ":path": "/", ":scheme": "https", ":authority": "localhost"]
        let handle = try connection.openStream(headers: headers, endStream: false) { _ in state }
        _ = connection.withStream(handle) { stream, state in Self.write(&stream, state) }
        return handle
    }
}

/// Closes its channel from `makeStreamState`, as an error handler would on a failed write.
struct ClosingServerConsumer: NIOHTTP2ServerStreamConsumer {
    struct StreamState {}
    let channel: EmbeddedChannel
    let log: ConsumerLog

    mutating func makeStreamState(for stream: inout NIOHTTP2Stream) -> StreamState {
        self.channel.close(promise: nil)
        return StreamState()
    }

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<ClosingServerConsumer>) {
        streams.forEachReady { stream, _ in
            if stream.events.contains(.closed) {
                self.log.closeReasons.append(stream.closeReason)
            }
        }
    }
}

/// What a client writes first, then `frames`, encoded by hand (no client handler to keep them valid).
func rawClientBytes(_ frames: [HTTP2Frame]) throws -> ByteBuffer {
    var buffer = ByteBuffer(string: "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    var encoder = HTTP2FrameEncoder(allocator: ByteBufferAllocator())
    for frame in [HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))] + frames {
        if let extra = try encoder.encode(frame: frame, to: &buffer), case .byteBuffer(var bytes) = extra {
            buffer.writeBuffer(&bytes)
        }
    }
    return buffer
}

let rawRequestHeaders: HPACKHeaders = [":method": "POST", ":path": "/", ":scheme": "https", ":authority": "localhost"]

/// Writes the response head from `makeStreamState`, during the read, and ends the stream in its first visit.
struct EarlyHeadServerConsumer: NIOHTTP2ServerStreamConsumer {
    struct StreamState {}

    mutating func makeStreamState(for stream: inout NIOHTTP2Stream) -> StreamState {
        try? stream.writeHeaders([":status": "200"])
        return StreamState()
    }

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<EarlyHeadServerConsumer>) {
        streams.forEachReady { stream, _ in
            while stream.nextInboundPart() != nil {}
            if stream.isInboundComplete, !stream.isOutboundComplete { try? stream.writeEnd() }
        }
    }
}

/// Counts the flushes that reach the socket.
final class ConsumerFlushCounter: ChannelOutboundHandler {
    typealias OutboundIn = Any
    var flushes = 0

    func flush(context: ChannelHandlerContext) {
        self.flushes += 1
        context.flush()
    }
}

@Suite("HTTP/2 stream consumer")
struct StreamConsumerTests {
    @Test("A request without a body gets its response")
    func requestWithoutBody() throws {
        let pair = try ConsumerPair()
        let observed = ObservedStream()
        _ = try pair.open(observed, endStream: true)
        try pair.interact()

        #expect(observed.status == "200")
        #expect(observed.events.contains(.closed))
        if case .ended = observed.closeReason {
        } else {
            Issue.record("closed with \(String(describing: observed.closeReason))")
        }
    }

    @Test("A body is echoed, and a large one needs WINDOW_UPDATEs on both sides")
    func echoBody() throws {
        let pair = try ConsumerPair()
        let observed = ObservedStream()
        let body = ByteBuffer(repeating: 0x41, count: 100_000)
        let handle = try pair.open(observed, endStream: false)
        try pair.connection.withStream(handle) { stream, _ in
            try stream.writeData(body)
            try stream.writeEnd()
        }
        try pair.interact()

        #expect(observed.status == "200")
        #expect(observed.body == body)
        #expect(observed.events.contains(.localEndSent))
    }

    @Test(
        "A body that arrives whole is echoed without a WINDOW_UPDATE after the peer's end",
        arguments: [5, 40_000, 50_000, 60_000]
    )
    func bodyArrivingInOneRead(size: Int) throws {
        let pair = try ConsumerPair()
        let observed = ObservedStream()
        let body = ByteBuffer(repeating: 0x42, count: size)
        let handle = try pair.open(observed, endStream: false)
        try pair.connection.withStream(handle) { stream, _ in
            try stream.writeData(body)
            try stream.writeEnd()
        }
        // HEADERS and the whole body reach the server as one read.
        var batch = ByteBuffer()
        while let data = try pair.client.readOutbound(as: IOData.self) {
            if case .byteBuffer(var buffer) = data { batch.writeBuffer(&buffer) }
        }
        try pair.server.writeInbound(batch)
        try pair.interact()

        #expect(observed.status == "200")
        #expect(observed.body == body)
        if case .ended = observed.closeReason {} else {
            Issue.record("closed with \(String(describing: observed.closeReason))")
        }
    }

    @Test("A HEADERS block nio-http2 can't encode closes the stream instead of leaving it open")
    func unencodableHeadersCloseTheStream() throws {
        let pair = try ConsumerPair()
        let observed = ObservedStream()
        // Larger than SETTINGS_MAX_FRAME_SIZE once HPACK-encoded, and nio-http2 doesn't write CONTINUATION frames.
        var headers: HPACKHeaders = [":method": "GET", ":path": "/", ":scheme": "https", ":authority": "localhost"]
        headers.add(name: "x-big", value: String(repeating: "a", count: 40_000), indexing: .neverIndexed)
        _ = try pair.connection.openStream(headers: headers, endStream: true) { _ in observed }
        try pair.interact()

        #expect(observed.events.contains(.closed))
        if case .writeFailed = observed.closeReason {} else {
            Issue.record("closed with \(String(describing: observed.closeReason))")
        }
        #expect(pair.clientCore.openLocalStreams == 0)
    }

    @Test("A write and the end in separate accesses of one batch leave as one DATA frame with END_STREAM")
    func endFoldsAcrossAccessesOfABatch() throws {
        let pair = try ConsumerPair()
        let handle = try pair.open(ObservedStream(), endStream: false)
        _ = try pair.drainClientFrames()

        pair.connection.withStreams { streams in
            _ = try? streams.withStream(handle) { stream, _ in try stream.writeData(ByteBuffer(string: "hello")) }
            _ = try? streams.withStream(handle) { stream, _ in try stream.writeEnd() }
        }

        let data = try pair.drainClientFrames().compactMap { frame -> (Int, Bool)? in
            guard case .data(let payload) = frame.payload else { return nil }
            return (payload.data.readableBytes, payload.endStream)
        }
        #expect(data.count == 1)
        #expect(data.first?.0 == 5)
        #expect(data.first?.1 == true)
    }

    @Test("Root-stream frames still reach the pipeline")
    func rootFramesReachThePipeline() throws {
        let pair = try ConsumerPair()
        let settings = pair.recorder.frames.filter {
            if case .settings = $0.payload { return true }
            return false
        }
        #expect(!settings.isEmpty)
        #expect(pair.recorder.frames.allSatisfy { $0.streamID == .rootStream })
    }

    @Test("Requests that arrive in one read batch are served in one visit")
    func oneVisitPerReadBatch() throws {
        let pair = try ConsumerPair()
        let visitsBefore = pair.serverLog.visits
        let observed = (0..<5).map { _ in ObservedStream() }
        for stream in observed {
            _ = try pair.open(stream)
        }
        // All five HEADERS leave in one flush per open; deliver them to the server as one read.
        var batch = ByteBuffer()
        while let data = try pair.client.readOutbound(as: IOData.self) {
            if case .byteBuffer(var buffer) = data { batch.writeBuffer(&buffer) }
        }
        try pair.server.writeInbound(batch)
        #expect(pair.serverLog.visits == visitsBefore + 1)
        // Each stream twice: its request, then its close (the server's END closes it inside the same visit).
        #expect(pair.serverLog.streamsPerVisit.last == 10)
        try pair.interact()
        #expect(observed.allSatisfy { $0.status == "200" })
    }

    @Test("A reset before the flush never reports the end as sent")
    func resetBeforeFlush() throws {
        let pair = try ConsumerPair()
        let observed = ObservedStream()
        let handle = try pair.open(observed, endStream: false)
        try pair.connection.withStream(handle) { stream, _ in
            try stream.writeData(ByteBuffer(string: "hello"))
            try stream.writeEnd()
            stream.reset(.cancel)
        }
        try pair.interact()

        #expect(!observed.events.contains(.localEndSent))
        #expect(observed.events.contains(.closed))
        if case .reset(.cancel, byPeer: false) = observed.closeReason {
        } else {
            Issue.record("closed with \(String(describing: observed.closeReason))")
        }
    }

    @Test("Opening stops at the peer's MAX_CONCURRENT_STREAMS")
    func concurrencyLimit() throws {
        let pair = try ConsumerPair(serverSettings: [HTTP2Setting(parameter: .maxConcurrentStreams, value: 2)])
        #expect(pair.connection.openableStreamCount == 2)
        _ = try pair.open(ObservedStream(), endStream: false)
        _ = try pair.open(ObservedStream(), endStream: false)
        #expect(pair.connection.openableStreamCount == 0)
        #expect(throws: NIOHTTP2StreamError.self) { try pair.open(ObservedStream()) }
    }

    @Test("Stream IDs run out without trapping")
    func streamIDExhaustion() throws {
        let pair = try ConsumerPair()
        pair.clientCore.nextStreamID = Int(Int32.max)
        #expect(pair.connection.openableStreamCount == 1)
        let handle = try pair.open(ObservedStream())
        _ = handle
        #expect(pair.connection.openableStreamCount == 0)
        do {
            _ = try pair.open(ObservedStream())
            Issue.record("opened a stream past the last ID")
        } catch {
            if case .streamIDsExhausted = error {} else { Issue.record("threw \(error)") }
        }
    }

    @Test("Opening before the channel is active throws")
    func openBeforeActive() throws {
        let pair = try ConsumerPair(connect: false)
        #expect(pair.connection.openableStreamCount == 0)
        do {
            _ = try pair.open(ObservedStream())
            Issue.record("opened a stream before the channel was active")
        } catch {
            if case .connectionClosed = error {} else { Issue.record("threw \(error)") }
        }
    }

    @Test("GOAWAY refuses streams whose HEADERS are still buffered, once")
    func goAwayRefusesBufferedStreams() throws {
        let pair = try ConsumerPair()
        // Not writable: nio-http2 buffers new streams' HEADERS instead of creating the streams.
        pair.client.isWritable = false
        pair.client.pipeline.fireChannelWritabilityChanged()
        let observed = ObservedStream()
        _ = try pair.open(observed)
        #expect(pair.clientCore.openLocalStreams == 1)

        let goAway = HTTP2Frame(
            streamID: .rootStream,
            payload: .goAway(lastStreamID: .rootStream, errorCode: .noError, opaqueData: nil)
        )
        let encoder = HTTP2FrameEncoder(allocator: ByteBufferAllocator())
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        var frameEncoder = encoder
        _ = try frameEncoder.encode(frame: goAway, to: &buffer)
        try pair.client.writeInbound(buffer)

        #expect(observed.events.contains(.closed))
        if case .refused(lastStreamID: .rootStream) = observed.closeReason {
        } else {
            Issue.record("closed with \(String(describing: observed.closeReason))")
        }
        #expect(pair.clientCore.openLocalStreams == 0)
        #expect(pair.clientLog.connectionEvents.contains(.goAwayReceived))

        // A second GOAWAY finds nothing left to sweep.
        var again = ByteBufferAllocator().buffer(capacity: 64)
        _ = try frameEncoder.encode(frame: goAway, to: &again)
        try pair.client.writeInbound(again)
        #expect(pair.clientCore.openLocalStreams == 0)
        #expect(pair.connection.openableStreamCount == 0)
    }

    @Test("A slot freed while it was queued is never visited again, not even after reuse")
    func freedSlotLeavesTheReadyList() throws {
        let core = StreamConsumerCore(isClient: true, configuration: .init(), pipelineEvents: .none)
        let slot = core.allocateSlot(id: 1, isLocal: true)
        core.markReady(slot, .closed)
        let (dequeued, events) = try #require(core.dequeueReady())
        core.beginAccess(dequeued)
        // An event arriving during the stream's final access queues the slot again.
        core.markReady(dequeued, .readable)
        #expect(core.endAccess(dequeued, presented: events))
        #expect(core.dequeueReady() == nil)

        // The next stream in that slot is visited once, with its own events.
        let reused = core.allocateSlot(id: 3, isLocal: true)
        #expect(reused == slot)
        core.markReady(reused, .readable)
        #expect(core.dequeueReady()?.events == .readable)
        #expect(core.dequeueReady() == nil)
    }

    @Test("makeState runs inside an access of its stream, on clients and servers")
    func makeStateIsAnAccess() throws {
        let pair = try ConsumerPair()
        var accessed: Bool?
        let headers: HPACKHeaders = [":method": "GET", ":path": "/", ":scheme": "https", ":authority": "localhost"]
        _ = try pair.connection.openStream(headers: headers, endStream: true) { stream in
            accessed = stream.core.records[stream.slot].isAccessed
            return ObservedStream()
        }
        #expect(accessed == true)

        // Servers: the peer's stream is made inside an access too.
        try pair.interact()
        #expect(pair.serverLog.makeStateWasAccess == [true])
    }

    @Test("sendGoAway names the highest stream the peer opened")
    func sendGoAwayLastStreamID() throws {
        let pair = try ConsumerPair()
        _ = try pair.open(ObservedStream(), endStream: false)
        _ = try pair.open(ObservedStream(), endStream: false)
        try pair.interact()
        pair.serverConnection.sendGoAway(errorCode: .noError)
        try pair.interact()

        let lastStreamIDs = pair.recorder.frames.compactMap { frame -> HTTP2StreamID? in
            if case .goAway(let lastStreamID, _, _) = frame.payload { return lastStreamID }
            return nil
        }
        #expect(lastStreamIDs == [3])
    }

    @Test("A writer revived by .writable after every flush keeps going past the visit rounds of one pass")
    func writerOutlastsTheVisitRounds() throws {
        let (channel, connection) = try ChunkWriterConsumer.connect(high: 1024, low: 512)
        // About 20 rounds of two chunks each, well past one pass's rounds; within the default 64 KiB window.
        let state = ChunkWriterConsumer.StreamState(left: 40_000, chunk: 1000)
        _ = try ChunkWriterConsumer.open(connection, state)
        channel.embeddedEventLoop.run()
        #expect(state.left == 0)
    }

    @Test("Pending bytes exactly at the high watermark still bring .writable back")
    func exactlyAtTheHighWatermark() throws {
        let (channel, connection) = try ChunkWriterConsumer.connect(high: 1024, low: 512)
        let state = ChunkWriterConsumer.StreamState(left: 4096, chunk: 1024)
        _ = try ChunkWriterConsumer.open(connection, state)
        channel.embeddedEventLoop.run()
        #expect(state.left == 0)
    }

    @Test("A channel closed from makeStreamState gives the stream its .closed visit, after its state exists")
    func closeInsideMakeStreamState() async {
        await #expect(processExitsWith: .success) {
            let channel = EmbeddedChannel()
            let log = ConsumerLog()
            _ = try channel.pipeline.syncOperations.configureHTTP2Connection { _ in
                ClosingServerConsumer(channel: channel, log: log)
            }
            try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 0)).wait()
            let headers = HTTP2Frame(streamID: 1, payload: .headers(.init(headers: rawRequestHeaders, endStream: true)))
            _ = try? channel.writeInbound(try rawClientBytes([headers]))
            channel.embeddedEventLoop.run()
            precondition(log.closeReasons.count == 1, "expected one .closed visit, saw \(log.closeReasons.count)")
        }
    }

    @Test("A stream nio-http2 resets itself, on a bad inbound frame, isn't reported as reset by the peer")
    func handlerResetIsLocal() throws {
        let server = EmbeddedChannel()
        let log = ConsumerLog()
        _ = try server.pipeline.syncOperations.configureHTTP2Connection { _ in EchoServerConsumer(log: log) }
        try server.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 0)).wait()
        var headers = rawRequestHeaders
        headers.add(name: "content-length", value: "1")
        // Five bytes against a content-length of one: nio-http2 resets the stream with PROTOCOL_ERROR.
        let frames = [
            HTTP2Frame(streamID: 1, payload: .headers(.init(headers: headers))),
            HTTP2Frame(
                streamID: 1,
                payload: .data(.init(data: .byteBuffer(ByteBuffer(repeating: 0x41, count: 5)), endStream: true))
            ),
        ]
        _ = try? server.writeInbound(try rawClientBytes(frames))

        guard case .some(.some(.reset(let code, let byPeer))) = log.closeReasons.first else {
            Issue.record("closed with \(log.closeReasons)")
            return
        }
        #expect(code == .protocolError)
        #expect(!byPeer)
    }

    @Test("On a server, the client cancelling a stream after its GOAWAY is a reset, not a refusal")
    func peerCancelAfterItsGoAwayIsAReset() throws {
        let pair = try ConsumerPair()
        let handle = try pair.open(ObservedStream(), endStream: false)
        try pair.interact()
        pair.connection.sendGoAway(errorCode: .noError)
        try pair.interact()
        _ = pair.connection.withStream(handle) { stream, _ in stream.reset(.cancel) }
        try pair.interact()

        guard case .some(.some(.reset(.cancel, byPeer: true))) = pair.serverLog.closeReasons.first else {
            Issue.record("closed with \(pair.serverLog.closeReasons)")
            return
        }
    }

    @Test("completeWhenWritten after the end was sent succeeds at once")
    func completeWhenWrittenAfterTheEnd() throws {
        let pair = try ConsumerPair()
        let handle = try pair.open(ObservedStream(), endStream: false)
        _ = pair.connection.withStream(handle) { stream, _ in try? stream.writeEnd() }
        let promise = pair.client.eventLoop.makePromise(of: Void.self)
        var succeeded = false
        promise.futureResult.whenSuccess { succeeded = true }
        _ = pair.connection.withStream(handle) { stream, _ in stream.completeWhenWritten(promise) }
        #expect(succeeded)
        try pair.interact()
    }

    @Test("A read batch that needs automatic frames and a response leaves in one flush")
    func oneFlushPerReadBatch() throws {
        let server = EmbeddedChannel()
        let counter = ConsumerFlushCounter()
        try server.pipeline.syncOperations.addHandler(counter)
        _ = try server.pipeline.syncOperations.configureHTTP2Connection { _ in EchoServerConsumer(log: ConsumerLog()) }
        try server.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 0)).wait()
        counter.flushes = 0

        // The client's SETTINGS needs an ACK (an automatic frame), and the request gets its response in the visit.
        let headers = HTTP2Frame(streamID: 1, payload: .headers(.init(headers: rawRequestHeaders, endStream: true)))
        try server.writeInbound(try rawClientBytes([headers]))

        #expect(counter.flushes == 1)
        var written = ByteBuffer()
        while let data = try server.readOutbound(as: IOData.self) {
            if case .byteBuffer(var buffer) = data { written.writeBuffer(&buffer) }
        }
        #expect(written.readableBytes > 9 * 2, "SETTINGS ACK and the response")
    }

    @Test("Writes made during the read join the visit's flush, and the end folds into them")
    func writesDuringTheReadJoinTheVisit() throws {
        let server = EmbeddedChannel()
        let counter = ConsumerFlushCounter()
        try server.pipeline.syncOperations.addHandler(counter)
        _ = try server.pipeline.syncOperations.configureHTTP2Connection { _ in EarlyHeadServerConsumer() }
        try server.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 0)).wait()
        _ = try server.readOutbound(as: IOData.self)  // the server's preface
        counter.flushes = 0

        let headers = HTTP2Frame(streamID: 1, payload: .headers(.init(headers: rawRequestHeaders, endStream: true)))
        try server.writeInbound(try rawClientBytes([headers]))
        #expect(counter.flushes == 1)

        var decoder = HTTP2FrameDecoder(
            allocator: ByteBufferAllocator(),
            expectClientMagic: false,
            maximumSequentialContinuationFrames: 5
        )
        while let data = try server.readOutbound(as: IOData.self) {
            if case .byteBuffer(let buffer) = data { decoder.append(bytes: buffer) }
        }
        var streamFrames: [HTTP2Frame.FramePayload] = []
        while let (frame, _) = try decoder.nextFrame() {
            if frame.streamID == 1 { streamFrames.append(frame.payload) }
        }
        guard streamFrames.count == 1, case .headers(let response) = streamFrames[0] else {
            Issue.record("stream frames: \(streamFrames)")
            return
        }
        #expect(response.endStream)
    }

    @Test("Stale handles resolve to nothing")
    func staleHandle() throws {
        let pair = try ConsumerPair()
        let handle = try pair.open(ObservedStream())
        try pair.interact()
        let result = pair.connection.withStream(handle) { _, _ in true }
        #expect(result == nil)
    }
}

@Suite("Stream state pages")
struct StreamStatePagesTests {
    @Test("Addresses stay stable while pages are added")
    func stableAddresses() {
        let pages = StreamStatePages<Int>()
        let first = pages.pointer(at: 0)
        first.pointee = 42
        for index in 1..<200 {
            pages.pointer(at: index).pointee = index
        }
        #expect(pages.pointer(at: 0) == first)
        #expect(first.pointee == 42)
        #expect(pages.pointer(at: 199).pointee == 199)
        #expect(pages.capacity >= 200)
    }
}
