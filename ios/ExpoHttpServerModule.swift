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
    private var stopped = false
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

        Function("setup", setupHandler)
        Function("start", startHandler)
        Function("route", routeHandler)
        Function("respond", respondHandler)
        Function("respondBinary", respondBinaryHandler)
        Function("stop", stopHandler)
    }

    private func setupHandler(port: Int) {
        self.port = port;
    }

    private func startHandler() {
        NotificationCenter.default.addObserver(forName: UIApplication.willEnterForegroundNotification, object: nil, queue: .main) { [unowned self] notification in
            if (!self.stopped) {
                self.startServer(status: "RESUMED", message: "Server resumed")
            }
        }
        stopped = false;
        startServer(status: "STARTED", message: "Server started")
    }

    private func routeHandler(path: String, method: String, uuid: String) {
        server.add(path, block: { (req, res, next) in
            // Per-request uuid so concurrent connections don't clobber each
            // other's CRResponse in `self.responses`. The route-level `uuid`
            // is forwarded separately so JS can locate its handler.
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
                    "routeUuid": uuid,
                    "method": req.method.toString(),
                    "path": path,
                    "body": bodyString,
                    "headersJson": req.allHTTPHeaderFields.jsonString,
                    "paramsJson": req.query.jsonString,
                    "cookiesJson": req.cookies?.jsonString ?? "{}"
                ])
            }
        }, recursive: false, method: CRHTTPMethod.fromString(method))
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
        let tEnter = CFAbsoluteTimeGetCurrent()
        let data = Data(bytes: body.rawPointer, count: body.byteLength)
        let tCopied = CFAbsoluteTimeGetCurrent()
        let shortId = String(udid.prefix(8))
        DispatchQueue.main.async {
            let tMain = CFAbsoluteTimeGetCurrent()
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

    private func stopHandler() {
        stopped = true;
        stopServer(status: "STOPPED", message: "Server stopped")
    }

    private func startServer(status: String, message: String) {
        stopServer()
        if let port = port {
            // CRServer retains `delegate` weakly, so we hold the observer on
            // `self` and wire it before starting. The close callback may fire
            // on Criollo's delegate queue — marshal back to main where we
            // touch `uuidToConnection` / `connectionToUuids`.
            connectionObserver.onConnectionClose = { [weak self] connection in
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
            server.delegate = connectionObserver
            var error: NSError?
            server.startListening(&error, portNumber: UInt(port))
            if (error != nil) {
                sendEvent("onStatusUpdate", [
                    "status": "ERROR",
                    "message": error?.localizedDescription ?? "Unknown error starting server"
                ])
            } else {
                beginBackgroundTask()
                sendEvent("onStatusUpdate", [
                    "status": status,
                    "message": message
                ])
            }
        } else {
            sendEvent("onStatusUpdate", [
                "status": "ERROR",
                "message": "Can't start server with port configured"
            ])
        }
    }

    private func stopServer(status: String? = nil, message: String? = nil) {
        server.stopListening()
        endBackgroundTask()
        if let status = status, let message = message {
            sendEvent("onStatusUpdate", [
                "status": status,
                "message": message
            ])
        }
    }

    private func beginBackgroundTask() {
        if (bgTaskIdentifier == UIBackgroundTaskIdentifier.invalid) {
            self.bgTaskIdentifier = UIApplication.shared.beginBackgroundTask(withName: "BgTask", expirationHandler: {
                self.stopServer(status: "PAUSED", message: "Server paused")
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
        default:
            httpMethod = .get
        }
        return httpMethod
    }
}
