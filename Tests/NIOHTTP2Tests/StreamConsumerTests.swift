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
        StreamState()
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
        _ = try self.server.pipeline.syncOperations.configureHTTP2Connection(configuration: serverConfiguration) { _ in
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
        var pages = StreamStatePages<Int>()
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
