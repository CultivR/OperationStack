//
//  OperationStack.swift
//  OperationStack
//
//  Created by Jordan Kay on 2/8/17.
//  Copyright © 2017 Squareknot. All rights reserved.
//

private typealias Queue = [Operation]

public class OperationStack {
    fileprivate let workAvailableCondition = NSCondition()
    fileprivate let allWorkCompletedCondition = NSCondition()
    fileprivate let suspendedCondition = NSCondition()
    
    fileprivate var queues: [Queue] = Array(repeating: [], count: .priorityCount)
    fileprivate var threads: [Thread]!
    fileprivate var isSuspended = false
    fileprivate var setMaxConcurrentOperationCount: Int!
    
    public init(maxConcurrentOperationCount: Int = 1) {
        setMaxConcurrentOperationCount = maxConcurrentOperationCount
        
        let thread = Thread(target: self, selector: #selector(workThread), object: nil)
        thread.name = "thread-0"
        threads = [thread]
        thread.start()
    }
    
    deinit {
        stop()
        for index in 0..<queues.count {
            queues[index].removeAll()
        }
    }
    
    public func add(_ operation: Operation) {
        add(operation, atIndex: operation.queuePriority.index)
    }
    
    public func add(_ operation: @escaping () -> Void) {
        add(BlockOperation(block: operation))
    }
    
    public func add(operations: [Operation], awaitCompletion: Bool) {
        for operation in operations {
            add(operation)
        }
        if awaitCompletion {
            awaitAllOperationsCompleted()
        }
    }
    
    public func addLast(operation: Operation) {
        add(operation, atIndex: 0)
    }
    
    public func cancelAllOperations() {
        for operation in operations {
            operation.cancel()
        }
    }
}

private extension OperationStack {
    var operations: [Operation] {
        let operations: [Operation]
        objc_sync_enter(self)
        operations = queues.reduce([], +)
        objc_sync_exit(self)
        return operations
    }
    
    var operationCount: Int {
        let count: Int
        objc_sync_enter(self)
        count = queues.map { $0.count }.reduce(0, +)
        objc_sync_exit(self)
        return count
    }
    
    var suspended: Bool {
        get {
            suspendedCondition.lock()
            let result = isSuspended
            suspendedCondition.unlock()
            return result
        }
        set {
            if newValue {
                suspend()
            } else {
                resume()
            }
        }
    }
    
    var maxConcurrentOperationCount: Int {
        get {
            return setMaxConcurrentOperationCount
        }
        set {
            setMaxConcurrentOperationCount = (newValue < 0) ? 1 : newValue
            
            objc_sync_enter(self)
            var difference = setMaxConcurrentOperationCount - threads.count
            while difference > 0 {
                let thread = Thread(target: self, selector: #selector(workThread), object: nil)
                thread.name = "thread-\(threads.count)"
                threads.append(thread)
                thread.start()
                difference -= 1
            }
            
            while difference < 0 {
                let thread = threads.removeLast()
                thread.cancel()
                difference += 1
            }
            objc_sync_exit(self)
            
            workAvailableCondition.lock()
            workAvailableCondition.broadcast()
            workAvailableCondition.unlock()
        }
    }
    
    var hasAdditionalWork: Bool {
        for queue in queues {
            if queue.count > 0 {
                return true
            }
        }
        return false
    }
    
    func add(_ operation: Operation, atIndex index: Int) {
        workAvailableCondition.lock()
        objc_sync_enter(self)
        queues[index].append(operation)
        objc_sync_exit(self)
        workAvailableCondition.signal()
        workAvailableCondition.unlock()
    }
    
    func resume() {
        suspendedCondition.lock()
        if isSuspended {
            isSuspended = false
            suspendedCondition.broadcast()
        }
        suspendedCondition.unlock()
    }
    
    func suspend() {
        suspendedCondition.lock()
        isSuspended = true
        suspendedCondition.unlock()
    }
    
    func stop() {
        for thread in threads {
            thread.cancel()
        }
        resume()
        
        workAvailableCondition.lock()
        workAvailableCondition.broadcast()
        workAvailableCondition.unlock()
    }
    
    func awaitAllOperationsCompleted() {
        let isWorking: Bool
        workAvailableCondition.lock()
        isWorking = hasAdditionalWork
        workAvailableCondition.unlock()
        
        if isWorking {
            allWorkCompletedCondition.lock()
            allWorkCompletedCondition.wait()
            allWorkCompletedCondition.unlock()
        }
    }
    
    @objc dynamic func workThread() {
        autoreleasepool {
            var didRun = false
            let thread = Thread.current
            
            while !thread.isCancelled {
                suspendedCondition.lock()
                while isSuspended {
                    suspendedCondition.wait()
                }
                suspendedCondition.unlock()
                
                if !didRun {
                    workAvailableCondition.lock()
                    if !hasAdditionalWork {
                        allWorkCompletedCondition.lock()
                        allWorkCompletedCondition.broadcast()
                        allWorkCompletedCondition.unlock()
                    }
                    
                    while !hasAdditionalWork && !thread.isCancelled {
                        workAvailableCondition.wait()
                    }
                    workAvailableCondition.unlock()
                }
                
                if !thread.isCancelled {
                    var operation: Operation?
                    objc_sync_enter(self)
                    for index in 0..<queues.count {
                        operation = queues[index].last
                        if let operation = operation, operation.isReady {
                            queues[index].removeLast()
                            break
                        } else {
                            operation = nil
                        }
                    }
                    objc_sync_exit(self)
                    
                    if let operation = operation {
                        operation.start()
                        didRun = true
                    } else {
                        didRun = false
                    }
                }
            }
        }
    }
}

extension Operation.QueuePriority {
    var index: Int {
        return .priorityCount - rawValue - 1
    }
}

private extension Int {
    static let priorityCount = 5
}
