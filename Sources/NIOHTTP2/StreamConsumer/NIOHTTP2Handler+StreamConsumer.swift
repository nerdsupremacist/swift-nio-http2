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

/// Makes the core and the typed host once the handler is added.
internal typealias StreamConsumerFactory = (NIOHTTP2Handler, ChannelHandlerContext) -> StreamConsumerCore

internal struct TypedStreamConsumerFactory<Consumer: NIOHTTP2StreamConsumer & ~Copyable> {
    let box: StreamConnectionBox<Consumer>
    let isClient: Bool
    let configuration: NIOHTTP2Handler.Configuration
    let pipelineEvents: NIOHTTP2Handler.StreamConsumerPipelineEvents
    let makeConsumer: (NIOHTTP2StreamConnection<Consumer>) -> Consumer
    let makeState: ((inout Consumer, inout NIOHTTP2Stream) -> Consumer.StreamState)?

    func makeCore(handler: NIOHTTP2Handler, context: ChannelHandlerContext) -> StreamConsumerCore {
        let core = StreamConsumerCore(
            isClient: self.isClient,
            configuration: self.configuration,
            pipelineEvents: self.pipelineEvents
        )
        core.handler = handler
        core.context = context
        core.parentIsWritable = context.channel.isWritable
        let box = self.box
        box.eventLoop = context.eventLoop
        let consumer = self.makeConsumer(NIOHTTP2StreamConnection(box: box))
        let host = StreamConsumerHost(core: core, box: box, consumer: consumer, makeState: self.makeState)
        box.host = host
        core.host = host.calls
        return core
    }
}

extension NIOHTTP2Handler {
    /// Creates a client handler whose streams are served by a consumer, with no `Channel` per stream.
    ///
    /// `makeStreamConsumer` runs once, in `handlerAdded`. ``multiplexer`` and ``syncMultiplexer()`` throw.
    public convenience init<Consumer: NIOHTTP2ClientStreamConsumer & ~Copyable>(
        configuration: Configuration = .init(),
        pipelineEvents: StreamConsumerPipelineEvents = .none,
        frameDelegate: (any NIOHTTP2FrameDelegate)? = nil,
        makeStreamConsumer: @escaping (_ connection: NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) {
        self.init(
            client: StreamConnectionBox<Consumer>(),
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            frameDelegate: frameDelegate,
            makeStreamConsumer: makeStreamConsumer
        )
    }

    /// Creates a server handler whose streams are served by a consumer, with no `Channel` per stream.
    ///
    /// `makeStreamConsumer` runs once, in `handlerAdded`. ``multiplexer`` and ``syncMultiplexer()`` throw.
    public convenience init<Consumer: NIOHTTP2ServerStreamConsumer & ~Copyable>(
        configuration: Configuration = .init(),
        pipelineEvents: StreamConsumerPipelineEvents = .none,
        frameDelegate: (any NIOHTTP2FrameDelegate)? = nil,
        makeStreamConsumer: @escaping (_ connection: NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) {
        self.init(
            server: StreamConnectionBox<Consumer>(),
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            frameDelegate: frameDelegate,
            makeStreamConsumer: makeStreamConsumer
        )
    }

    internal convenience init<Consumer: NIOHTTP2ClientStreamConsumer & ~Copyable>(
        client box: StreamConnectionBox<Consumer>,
        configuration: Configuration,
        pipelineEvents: StreamConsumerPipelineEvents,
        frameDelegate: (any NIOHTTP2FrameDelegate)?,
        makeStreamConsumer: @escaping (NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) {
        self.init(mode: .client, configuration: configuration, frameDelegate: frameDelegate)
        let factory = TypedStreamConsumerFactory<Consumer>(
            box: box,
            isClient: true,
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            makeConsumer: makeStreamConsumer,
            makeState: nil
        )
        self.setStreamConsumerFactory { factory.makeCore(handler: $0, context: $1) }
    }

    internal convenience init<Consumer: NIOHTTP2ServerStreamConsumer & ~Copyable>(
        server box: StreamConnectionBox<Consumer>,
        configuration: Configuration,
        pipelineEvents: StreamConsumerPipelineEvents,
        frameDelegate: (any NIOHTTP2FrameDelegate)?,
        makeStreamConsumer: @escaping (NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) {
        self.init(mode: .server, configuration: configuration, frameDelegate: frameDelegate)
        let factory = TypedStreamConsumerFactory<Consumer>(
            box: box,
            isClient: false,
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            makeConsumer: makeStreamConsumer,
            makeState: { consumer, stream in consumer.makeStreamState(for: &stream) }
        )
        self.setStreamConsumerFactory { factory.makeCore(handler: $0, context: $1) }
    }
}

extension ChannelPipeline.SynchronousOperations {
    /// Adds a client consumer ``NIOHTTP2Handler`` and returns its connection. Event loop only.
    public func configureHTTP2Connection<Consumer: NIOHTTP2ClientStreamConsumer & ~Copyable>(
        configuration: NIOHTTP2Handler.Configuration = .init(),
        pipelineEvents: NIOHTTP2Handler.StreamConsumerPipelineEvents = .none,
        frameDelegate: (any NIOHTTP2FrameDelegate)? = nil,
        position: ChannelPipeline.SynchronousOperations.Position = .last,
        makeStreamConsumer: @escaping (_ connection: NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) throws -> NIOHTTP2StreamConnection<Consumer> {
        let box = StreamConnectionBox<Consumer>()
        let handler = NIOHTTP2Handler(
            client: box,
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            frameDelegate: frameDelegate,
            makeStreamConsumer: makeStreamConsumer
        )
        try self.addHandler(handler, position: position)
        return NIOHTTP2StreamConnection(box: box)
    }

    /// Adds a server consumer ``NIOHTTP2Handler`` and returns its connection. Event loop only.
    public func configureHTTP2Connection<Consumer: NIOHTTP2ServerStreamConsumer & ~Copyable>(
        configuration: NIOHTTP2Handler.Configuration = .init(),
        pipelineEvents: NIOHTTP2Handler.StreamConsumerPipelineEvents = .none,
        frameDelegate: (any NIOHTTP2FrameDelegate)? = nil,
        position: ChannelPipeline.SynchronousOperations.Position = .last,
        makeStreamConsumer: @escaping (_ connection: NIOHTTP2StreamConnection<Consumer>) -> Consumer
    ) throws -> NIOHTTP2StreamConnection<Consumer> {
        let box = StreamConnectionBox<Consumer>()
        let handler = NIOHTTP2Handler(
            server: box,
            configuration: configuration,
            pipelineEvents: pipelineEvents,
            frameDelegate: frameDelegate,
            makeStreamConsumer: makeStreamConsumer
        )
        try self.addHandler(handler, position: position)
        return NIOHTTP2StreamConnection(box: box)
    }
}
