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
    private let server = CRHTTPServer()
    private var port: Int?
    private var isListening = false
    /// True after an explicit JS `stop()`; suppresses the
    /// `OnAppEntersForeground` auto-resume until the next `ensureListening`.
    private var userStopped = false
    private var responses = [String: CRResponse]()
    private var bgTaskIdentifier = UIBackgroundTaskIdentifier.invalid

    // Track which request uuids are bound to each CRConnection so a TCP
    // close can fan out to `onRequestCancel` events for every pending
    // request on that connection (HTTP keep-alive may multiplex several).
    private var uuidToConnection = [String: ObjectIdentifier]()
    private var connectionToUuids = [ObjectIdentifier: Set<String>]()
    private let connectionObserver = CRConnectionObserver()

    public func definition() -> ModuleDefinition {
        Name("ExpoHttpServer")

        Events("onStatusUpdate", "onRequest", "onRequestCancel")

        // Process-scoped wiring. Runs once per native module construction,
        // which survives JS reloads — so routes and observers don't stack.
        OnCreate {
            self.connectionObserver.onConnectionClose = { [weak self] connection in
                let connId = ObjectIdentifier(connection)
                DispatchQueue.main.async {
                    guard let self = self else { return }
                    guard let uuids = self.connectionToUuids.removeValue(forKey: connId) else { return }
                    for uuid in uuids {
                        self.uuidToConnection[uuid] = nil
                        self.sendEvent("onRequestCancel", ["uuid": uuid])
                    }
                }
            }
            self.server.delegate = self.connectionObserver

            // One recursive catch-all per method; every request is dispatched
            // to JS, which owns method+path routing. CRHTTPMethod has no
            // `.all`, so we register four times — all forwarding to the same
            // block. Criollo's matcher hands each request to exactly one
            // route, so no fan-out within a single bind.
            let methods: [CRHTTPMethod] = [.get, .post, .put, .delete, .options]
            for method in methods {
                self.server.add("/", block: self.dispatchBlock, recursive: true, method: method)
            }
        }

        OnAppEntersBackground {
            self.beginBackgroundTask()
        }

        OnAppEntersForeground {
            self.endBackgroundTask()
            guard !self.isListening, !self.userStopped, let port = self.port else { return }
            self.bindAndEmit(
                port: port,
                successStatus: "RESUMED",
                successMessage: "Server resumed",
                retriesRemaining: 1
            )
        }

        OnDestroy {
            self.stopServer()
            self.endBackgroundTask()
        }

        AsyncFunction("ensureListening") { (port: Int, promise: Promise) in
            self.userStopped = false
            if self.isListening && self.port == port {
                promise.resolve()
                return
            }
            if self.isListening {
                self.server.stopListening()
                self.isListening = false
            }
            self.port = port
            if self.bindAndEmit(port: port, successStatus: "STARTED", successMessage: "Server started") {
                promise.resolve()
            } else {
                promise.reject(
                    "ERR_SERVER_START",
                    "Failed to bind HTTP server to port \(port)"
                )
            }
        }

        Function("respond", respondHandler)
        Function("respondBinary", respondBinaryHandler)

        AsyncFunction("stop") { (promise: Promise) in
            self.userStopped = true
            self.stopServer(status: "STOPPED", message: "Server stopped")
            promise.resolve()
        }
    }

    /// Single Criollo route block used for all methods. Forwards every
    /// request to JS via `onRequest`; JS dispatches to its handler and
    /// calls `respond`/`respondBinary` with the per-request uuid.
    private lazy var dispatchBlock: CRRouteBlock = { [weak self] req, res, _ in
        guard let self = self else { return }
        let requestUuid = UUID().uuidString
        DispatchQueue.main.async {
            var bodyString = "{}"
            if let body = req.body, let bodyData = try? JSONSerialization.data(withJSONObject: body) {
                bodyString = String(data: bodyData, encoding: .utf8) ?? "{}"
            }
            self.responses[requestUuid] = res
            if let connection = req.connection {
                let connId = ObjectIdentifier(connection)
                self.uuidToConnection[requestUuid] = connId
                self.connectionToUuids[connId, default: []].insert(requestUuid)
            }
            self.sendEvent("onRequest", [
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

    /// Attempts `startListening` on `port`. Emits the matching status event
    /// and returns whether bind succeeded.
    ///
    /// Self-guarded against double-bind: if the server is already listening
    /// (e.g. another caller bound first while this one was queued), returns
    /// true without re-binding or emitting a duplicate event. Lets the
    /// `ensureListening` AsyncFunction and the `OnAppEntersForeground`
    /// observer call this concurrently without serialization.
    ///
    /// `retriesRemaining` schedules a delayed retry on bind failure before
    /// emitting `ERROR`. The foreground-resume path uses one retry to ride
    /// over the brief window where iOS hasn't released the prior socket.
    @discardableResult
    private func bindAndEmit(
        port: Int,
        successStatus: String,
        successMessage: String,
        retriesRemaining: Int = 0,
        retryDelay: TimeInterval = 0.15
    ) -> Bool {
        if isListening { return true }
        var error: NSError?
        server.startListening(&error, portNumber: UInt(port))
        if let error = error {
            if retriesRemaining > 0 {
                DispatchQueue.main.asyncAfter(deadline: .now() + retryDelay) { [weak self] in
                    guard let self = self else { return }
                    guard !self.isListening, !self.userStopped else { return }
                    self.bindAndEmit(
                        port: port,
                        successStatus: successStatus,
                        successMessage: successMessage,
                        retriesRemaining: retriesRemaining - 1,
                        retryDelay: retryDelay
                    )
                }
                return false
            }
            sendEvent("onStatusUpdate", [
                "status": "ERROR",
                "message": error.localizedDescription
            ])
            return false
        }
        isListening = true
        sendEvent("onStatusUpdate", [
            "status": successStatus,
            "message": successMessage
        ])
        return true
    }

    private func stopServer(status: String? = nil, message: String? = nil) {
        if isListening {
            server.stopListening()
            isListening = false
        }
        if let status = status, let message = message {
            sendEvent("onStatusUpdate", [
                "status": status,
                "message": message
            ])
        }
    }

    private func beginBackgroundTask() {
        if (bgTaskIdentifier == UIBackgroundTaskIdentifier.invalid) {
            self.bgTaskIdentifier = UIApplication.shared.beginBackgroundTask(withName: "ExpoHttpServerBg", expirationHandler: { [weak self] in
                guard let self = self else { return }
                self.stopServer(status: "PAUSED", message: "Server paused")
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
