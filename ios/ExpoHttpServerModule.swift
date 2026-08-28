import ExpoModulesCore
import Criollo
import os.log

/// CRServerDelegate wrapper — we can't make `ExpoHttpServerModule` conform
/// directly because it extends Expo's `Module` (not NSObject). Criollo's
/// delegate is an ObjC `@objc` protocol inheriting from NSObject, so we use
/// a dedicated NSObject subclass and a closure to forward close events.
private class CRConnectionObserver: NSObject, CRServerDelegate {
    var onConnectionClose: ((CRConnection) -> Void)?

    // Swift translates CRServerDelegate's `-server:didCloseConnection:` to
    // `server(_:didClose:)` (the ObjC-to-Swift importer strips the redundant
    // "Connection" word matching the parameter's type name).
    func server(_ server: CRServer, didClose connection: CRConnection) {
        onConnectionClose?(connection)
    }
}

public class ExpoHttpServerModule: Module {
    // MARK: - Process-scoped state
    //
    // Expo builds one module object per AppContext, not per process:
    // `ModuleRegistry.register(moduleType:)` calls `moduleType.init(appContext:)`,
    // and `OnDestroy` only fires from `ModuleHolder.deinit` — whenever ARC gets
    // around to releasing the old holder, which is not ordered against the new
    // context's setup. A per-instance CRHTTPServer therefore means every JS
    // reload races a second server against the first for the same port; the
    // loser takes EADDRINUSE and, because the winner is a zombie bound to a
    // dead runtime, never recovers for the life of the process.
    //
    // The socket, its bind state and the catch-all routes are therefore
    // process-scoped and shared across contexts. Only per-request bookkeeping
    // (which CRResponse answers which uuid) stays on the instance, because it
    // belongs to the JS runtime that will produce the response.
    private static let server = CRHTTPServer()
    private static var isListening = false
    private static var listeningPort: Int?
    /// True after an explicit JS `stop()`; suppresses the
    /// `OnAppEntersForeground` auto-resume until the next `ensureListening`.
    private static var userStopped = false
    private static var routesRegistered = false
    private static let connectionObserver = CRConnectionObserver()

    /// The module belonging to the newest AppContext. Requests dispatch here so
    /// a reload hands them to the live JS runtime rather than a dead one, and
    /// lifecycle hooks fired on a stale instance can bail out.
    private static weak var activeModule: ExpoHttpServerModule?

    /// Bind attempts per `ensureListening` / resume, spaced by `bindRetryDelay`.
    /// Rides over the window where a prior socket is closing. JS owns the long
    /// backoff; this only covers the sub-second case.
    private static let bindAttempts = 4
    private static let bindRetryDelay: TimeInterval = 0.15

    // MARK: - Per-context state

    private var responses = [String: CRResponse]()
    private var bgTaskIdentifier = UIBackgroundTaskIdentifier.invalid

    // Track which request uuids are bound to each CRConnection so a TCP
    // close can fan out to `onRequestCancel` events for every pending
    // request on that connection (HTTP keep-alive may multiplex several).
    private var uuidToConnection = [String: ObjectIdentifier]()
    private var connectionToUuids = [ObjectIdentifier: Set<String>]()

    private var isActive: Bool { ExpoHttpServerModule.activeModule === self }

    public func definition() -> ModuleDefinition {
        Name("ExpoHttpServer")

        Events("onStatusUpdate", "onRequest", "onRequestCancel")

        OnCreate {
            // Newest context wins: in-flight requests and every subsequent one
            // go to the runtime that can actually answer them.
            ExpoHttpServerModule.activeModule = self

            ExpoHttpServerModule.connectionObserver.onConnectionClose = { connection in
                let connId = ObjectIdentifier(connection)
                DispatchQueue.main.async {
                    guard let module = ExpoHttpServerModule.activeModule else { return }
                    guard let uuids = module.connectionToUuids.removeValue(forKey: connId) else { return }
                    for uuid in uuids {
                        module.uuidToConnection[uuid] = nil
                        module.sendEvent("onRequestCancel", ["uuid": uuid])
                    }
                }
            }
            ExpoHttpServerModule.server.delegate = ExpoHttpServerModule.connectionObserver

            // One recursive catch-all per method; every request is dispatched
            // to JS, which owns method+path routing. CRHTTPMethod has no
            // `.all`, so we register five times — all forwarding to the same
            // block. Criollo's matcher hands each request to exactly one
            // route, so no fan-out within a single bind. Registered once per
            // process because the server they hang off is process-scoped.
            if !ExpoHttpServerModule.routesRegistered {
                ExpoHttpServerModule.routesRegistered = true
                let methods: [CRHTTPMethod] = [.get, .post, .put, .delete, .options]
                for method in methods {
                    ExpoHttpServerModule.server.add(
                        "/",
                        block: ExpoHttpServerModule.dispatchBlock,
                        recursive: true,
                        method: method
                    )
                }
            }
        }

        OnAppEntersBackground {
            guard self.isActive else { return }
            self.beginBackgroundTask()
        }

        OnAppEntersForeground {
            guard self.isActive else { return }
            self.endBackgroundTask()
            guard !ExpoHttpServerModule.isListening,
                  !ExpoHttpServerModule.userStopped,
                  let port = ExpoHttpServerModule.listeningPort else { return }
            self.bindWithRetry(
                port: port,
                successStatus: "RESUMED",
                successMessage: "Server resumed",
                attemptsRemaining: ExpoHttpServerModule.bindAttempts
            ) { _ in }
        }

        OnDestroy {
            self.endBackgroundTask()
            // A reload has already installed a newer module as the active one,
            // and the shared socket belongs to it now — tearing it down here is
            // exactly the race this class used to lose. Only the last context
            // standing stops the server.
            guard ExpoHttpServerModule.activeModule == nil || self.isActive else { return }
            ExpoHttpServerModule.activeModule = nil
            ExpoHttpServerModule.stopServer()
        }

        AsyncFunction("ensureListening") { (port: Int, promise: Promise) in
            ExpoHttpServerModule.userStopped = false
            if ExpoHttpServerModule.isListening && ExpoHttpServerModule.listeningPort == port {
                promise.resolve()
                return
            }
            if ExpoHttpServerModule.isListening {
                ExpoHttpServerModule.stopServer()
            }
            ExpoHttpServerModule.listeningPort = port
            self.bindWithRetry(
                port: port,
                successStatus: "STARTED",
                successMessage: "Server started",
                attemptsRemaining: ExpoHttpServerModule.bindAttempts
            ) { bound in
                if bound {
                    promise.resolve()
                } else {
                    promise.reject(
                        "ERR_SERVER_START",
                        "Failed to bind HTTP server to port \(port)"
                    )
                }
            }
        }
        // All bind-state mutation happens on the main queue so the async retry
        // hops and the lifecycle hooks can't interleave with a JS-driven bind.
        .runOnQueue(DispatchQueue.main)

        Function("respond", respondHandler)
        Function("respondBinary", respondBinaryHandler)

        AsyncFunction("stop") { (promise: Promise) in
            ExpoHttpServerModule.userStopped = true
            ExpoHttpServerModule.stopServer()
            self.sendEvent("onStatusUpdate", ["status": "STOPPED", "message": "Server stopped"])
            promise.resolve()
        }
        .runOnQueue(DispatchQueue.main)
    }

    /// Single Criollo route block used for all methods and every context.
    /// Forwards each request to whichever JS runtime is current; that runtime
    /// dispatches to its handler and calls `respond`/`respondBinary` with the
    /// per-request uuid.
    private static let dispatchBlock: CRRouteBlock = { req, res, _ in
        let requestUuid = UUID().uuidString
        DispatchQueue.main.async {
            guard let module = ExpoHttpServerModule.activeModule else {
                // No live JS runtime to answer. Fail the request rather than
                // leaving the connection hanging until the client times out.
                res.setStatusCode(503, description: "Service Unavailable")
                res.setValue("application/json", forHTTPHeaderField: "Content-type")
                res.send("{\"error\":\"No JS runtime\"}")
                return
            }
            var bodyString = "{}"
            if let body = req.body, let bodyData = try? JSONSerialization.data(withJSONObject: body) {
                bodyString = String(data: bodyData, encoding: .utf8) ?? "{}"
            }
            module.responses[requestUuid] = res
            if let connection = req.connection {
                let connId = ObjectIdentifier(connection)
                module.uuidToConnection[requestUuid] = connId
                module.connectionToUuids[connId, default: []].insert(requestUuid)
            }
            module.sendEvent("onRequest", [
                "uuid": requestUuid,
                "method": req.method.toString(),
                "path": req.url.path,
                "body": bodyString,
                "headersJson": req.allHTTPHeaderFields.jsonString,
                "paramsJson": req.query.jsonString,
                "cookiesJson": req.cookies?.jsonString ?? "{}"
            ])
        }
    }

    private func respondHandler(udid: String,
                                statusCode: Int,
                                statusDescription: String,
                                contentType: String,
                                headers: [String: String],
                                body: String) {
        let byteLength = body.lengthOfBytes(using: .utf8)
        DispatchQueue.main.async {
            if let response = self.responses[udid] {
                response.setStatusCode(UInt(statusCode), description: statusDescription)
                response.setValue(contentType, forHTTPHeaderField: "Content-type")
                response.setValue("\(byteLength)", forHTTPHeaderField: "Content-Length")
                for (key, value) in headers {
                    response.setValue(value, forHTTPHeaderField: key)
                }
                response.send(body)
                self.clearRequestTracking(udid: udid)
            }
        }
    }

    private func respondBinaryHandler(udid: String,
                                      statusCode: Int,
                                      statusDescription: String,
                                      contentType: String,
                                      headers: [String: String],
                                      body: Uint8Array) {
        // Copy the typed-array bytes into Data on the JS thread — the backing
        // JavaScript ArrayBuffer may not be safe to touch once we hop to the
        // main queue, and CRResponse.sendData retains the Data itself.
        let data = Data(bytes: body.rawPointer, count: body.byteLength)
        DispatchQueue.main.async {
            if let response = self.responses[udid] {
                response.setStatusCode(UInt(statusCode), description: statusDescription)
                response.setValue(contentType, forHTTPHeaderField: "Content-type")
                response.setValue("\(data.count)", forHTTPHeaderField: "Content-Length")
                for (key, value) in headers {
                    response.setValue(value, forHTTPHeaderField: key)
                }
                response.send(data)
                self.clearRequestTracking(udid: udid)
            }
        }
    }

    /// Must be called on the main queue. Clears both the CRResponse map and
    /// the connection-tracking maps for a completed request.
    private func clearRequestTracking(udid: String) {
        self.responses[udid] = nil
        if let connId = self.uuidToConnection.removeValue(forKey: udid) {
            if var uuids = self.connectionToUuids[connId] {
                uuids.remove(udid)
                if uuids.isEmpty {
                    self.connectionToUuids[connId] = nil
                } else {
                    self.connectionToUuids[connId] = uuids
                }
            }
        }
    }

    /// Attempts `startListening` on `port`, retrying `attemptsRemaining - 1`
    /// times at `bindRetryDelay` spacing before giving up. Emits the matching
    /// status event and calls `completion` with the outcome — including for a
    /// retry that only succeeds later, which the previous fire-and-forget
    /// version reported as a failure.
    ///
    /// Main queue only. Self-guarded against double-bind: if the socket is
    /// already up (another caller won the race while this one was queued),
    /// succeeds without re-binding or emitting a duplicate event.
    private func bindWithRetry(
        port: Int,
        successStatus: String,
        successMessage: String,
        attemptsRemaining: Int,
        completion: @escaping (Bool) -> Void
    ) {
        if ExpoHttpServerModule.isListening {
            completion(true)
            return
        }
        if ExpoHttpServerModule.userStopped {
            completion(false)
            return
        }
        var error: NSError?
        ExpoHttpServerModule.server.startListening(&error, portNumber: UInt(port))
        if error == nil {
            ExpoHttpServerModule.isListening = true
            sendEvent("onStatusUpdate", [
                "status": successStatus,
                "message": successMessage
            ])
            completion(true)
            return
        }
        guard attemptsRemaining > 1 else {
            sendEvent("onStatusUpdate", [
                "status": "ERROR",
                "message": error?.localizedDescription ?? "Failed to bind to port \(port)"
            ])
            completion(false)
            return
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + ExpoHttpServerModule.bindRetryDelay) {
            [weak self] in
            guard let self = self else {
                completion(false)
                return
            }
            self.bindWithRetry(
                port: port,
                successStatus: successStatus,
                successMessage: successMessage,
                attemptsRemaining: attemptsRemaining - 1,
                completion: completion
            )
        }
    }

    private static func stopServer() {
        if isListening {
            server.stopListening()
            isListening = false
        }
    }

    private func beginBackgroundTask() {
        if (bgTaskIdentifier == UIBackgroundTaskIdentifier.invalid) {
            self.bgTaskIdentifier = UIApplication.shared.beginBackgroundTask(withName: "ExpoHttpServerBg", expirationHandler: { [weak self] in
                guard let self = self else { return }
                ExpoHttpServerModule.stopServer()
                self.sendEvent("onStatusUpdate", ["status": "PAUSED", "message": "Server paused"])
                self.endBackgroundTask()
            })
        }
    }

    private func endBackgroundTask() {
        if (bgTaskIdentifier != UIBackgroundTaskIdentifier.invalid) {
            UIApplication.shared.endBackgroundTask(bgTaskIdentifier)
            bgTaskIdentifier = UIBackgroundTaskIdentifier.invalid
        }
    }
}

extension Dictionary {
    var jsonString: String {
        guard let data = try? JSONSerialization.data(withJSONObject: self) else {
            return "{}";
        }
        return String(data: data, encoding: .utf8) ?? "{}"
    }
}

extension CRHTTPMethod {
    func toString() -> String {
        switch self {
        case .post:
            return "POST"
        case .put:
            return "PUT"
        case .delete:
            return "DELETE"
        case .options:
            return "OPTIONS"
        default:
            return "GET"
        }
    }

    static func fromString(_ string: String) -> Self {
        var httpMethod: CRHTTPMethod
        switch (string) {
        case "POST":
            httpMethod = .post
        case "PUT":
            httpMethod = .put
        case "DELETE":
            httpMethod = .delete
        case "OPTIONS":
            httpMethod = .options
        default:
            httpMethod = .get
        }
        return httpMethod
    }
}
