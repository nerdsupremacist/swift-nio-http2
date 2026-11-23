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

/// Index-addressed storage of optional values in pages that are allocated once and never moved.
///
/// A value's address is stable until the storage is destroyed, so a value can be lent `inout` while other slots are
/// added (a consumer may open a stream while it's visiting another). Like swift-nio-quic's `PagedBuffer`, this is not
/// safe by itself: callers must not keep a pointer beyond the access it was made for.
///
/// A class rather than a `~Copyable` struct, so its holder keeps it in a `let`: every access then skips a dynamic
/// exclusivity check, where a struct's `mutating` access to the holder's `var` pays one. A stored noncopyable struct
/// also crashes the release optimizer of Swift 6.1 to 6.3 in `@inlinable` code.
@usableFromInline
internal final class StreamStatePages<Element> {
    @usableFromInline
    internal static var pageSize: Int { 32 }

    @usableFromInline
    internal var pages: [UnsafeMutablePointer<Element?>] = []

    internal init() {}

    internal var capacity: Int {
        self.pages.count * Self.pageSize
    }

    /// The slot's storage, allocating its page first if needed. Valid until the storage is destroyed.
    @inlinable
    internal func pointer(at index: Int) -> UnsafeMutablePointer<Element?> {
        precondition(index >= 0)
        let page = index / Self.pageSize
        while self.pages.count <= page {
            let newPage = UnsafeMutablePointer<Element?>.allocate(capacity: Self.pageSize)
            newPage.initialize(repeating: nil, count: Self.pageSize)
            self.pages.append(newPage)
        }
        return self.pages[page] + (index % Self.pageSize)
    }

    deinit {
        for page in self.pages {
            page.deinitialize(count: Self.pageSize)
            page.deallocate()
        }
    }
}

@available(*, unavailable)
extension StreamStatePages: Sendable {}
