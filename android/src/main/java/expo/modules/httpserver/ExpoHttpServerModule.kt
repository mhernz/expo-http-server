package expo.modules.httpserver

import androidx.core.os.bundleOf
import expo.modules.kotlin.Promise
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import expo.modules.kotlin.typedarray.Uint8Array
import com.safframework.server.core.AndroidServer
import com.safframework.server.core.Server
import com.safframework.server.core.http.HttpMethod
import com.safframework.server.core.http.Request
import com.safframework.server.core.http.Response
import org.json.JSONObject
import java.lang.ref.WeakReference
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ExpoHttpServerModule : Module() {
  class SimpleHttpResponse(val statusCode: Int,
                           val statusDescription: String,
                           val contentType: String,
                           val headers: HashMap<String, String>,
                           val bodyText: String?,
                           val bodyBytes: ByteArray?)

  // Process-scoped state.
  //
  // Expo builds one module object per AppContext, not per process, and
  // OnDestroy fires when the old holder is collected — which is not ordered
  // against the new context's setup. Per-instance server state therefore means
  // every JS reload races a second Netty server against the first for the same
  // port; the loser takes EADDRINUSE and, because the winner is bound to a dead
  // runtime, never recovers for the life of the process.
  //
  // The socket, its bind state and the in-flight request maps are shared across
  // contexts, and requests are dispatched to whichever module is current.
  companion object {
    private var server: Server? = null
    private var listeningPort: Int? = null
    /** True after an explicit JS `stop()`; suppresses `OnActivityEntersForeground`
     *  auto-resume until the next `ensureListening`. Mirrors iOS. */
    private var userStopped = false

    /** The module belonging to the newest AppContext. */
    private var activeModule = WeakReference<ExpoHttpServerModule>(null)

    /**
     * Bumped when a new AppContext takes over. A dispatch thread parked on a
     * response from the previous runtime watches this so a reload releases it
     * instead of pinning a Netty worker until the process dies.
     */
    private val generation = AtomicInteger(0)

    // ConcurrentHashMap because route lambdas run on Netty worker threads while
    // respondBinary/respond run on the JS thread — both touch these maps.
    private val responses = ConcurrentHashMap<String, SimpleHttpResponse>()
    // Per-request cancellation flag. Set when the underlying Netty channel
    // closes (client disconnect) so the busy-wait loop can exit instead of
    // pinning a worker thread for tiles no one is waiting for.
    private val cancelled = ConcurrentHashMap<String, Boolean>()

    /**
     * Bind attempts per `ensureListening`, spaced by the delay below. Rides
     * over the window where a prior socket is closing. JS owns the long
     * backoff; this only covers the sub-second case.
     */
    private const val BIND_ATTEMPTS = 4
    private const val BIND_RETRY_DELAY_MS = 150L
    /** The resume path runs on the UI thread, so it stays cheap and leaves the
     *  long tail to the JS backoff loop, which reacts to the ERROR event. */
    private const val RESUME_BIND_ATTEMPTS = 2

    /** Guards every mutation of `server` — the JS thread and the activity
     *  lifecycle callbacks both get there. */
    private val bindLock = Any()

    /**
     * The single Netty route handler, shared by every context. Forwards each
     * request to whichever JS runtime is current and waits for
     * `respond`/`respondBinary` to populate `responses[uuid]`, for the channel
     * to close (client disconnect), or for a reload to strand it.
     */
    private fun dispatchToJs(request: Request, response: Response): Response {
      val requestUuid = UUID.randomUUID().toString()
      val gen = generation.get()
      val module = activeModule.get()
      if (module == null) {
        // No live JS runtime to answer. Fail the request rather than parking
        // this worker until the client gives up.
        val payload = "{\"error\":\"No JS runtime\"}"
        response.setBodyText(payload)
        response.setStatus(503)
        response.addHeader("Content-Length", payload.toByteArray(Charsets.UTF_8).size.toString())
        response.addHeader("Content-Type", "application/json")
        return response
      }

      // Listen for socket close so MapLibre-native cancelling a tile (TCP
      // close before we respond) propagates to JS as `onRequestCancel`.
      // safframework's HttpResponse stores the Channel in a private field
      // with no accessor — reflection in extractChannel().
      val channel = extractChannel(response)
      channel?.closeFuture()?.addListener {
        if (responses.containsKey(requestUuid) || cancelled.putIfAbsent(requestUuid, true) == null) {
          cancelled[requestUuid] = true
          activeModule.get()?.sendEvent("onRequestCancel", bundleOf("uuid" to requestUuid))
        }
      }

      val headers: Map<String, String> = request.headers()
      val params: Map<String, String> = request.params()
      val cookies: Map<String, String> = request.cookies().associate { it.name() to it.value() }
      module.sendEvent("onRequest", bundleOf(
        "uuid" to requestUuid,
        "method" to request.method().name,
        "path" to request.url(),
        "body" to request.content(),
        "headersJson" to JSONObject(headers).toString(),
        "paramsJson" to JSONObject(params).toString(),
        "cookiesJson" to JSONObject(cookies).toString(),
      ))
      // Wait for JS to produce a response, OR for the client to disconnect, OR
      // for the runtime we asked to be replaced. Without these exits, abandoned
      // requests would pin this Netty worker thread indefinitely.
      while (!responses.containsKey(requestUuid) &&
             cancelled[requestUuid] != true &&
             generation.get() == gen) {
        Thread.sleep(10)
      }
      val res = responses[requestUuid]
      if (res == null) {
        cancelled.remove(requestUuid)
        if (generation.get() != gen) {
          // Reloaded out from under this request; the runtime that owned it is
          // gone and will never answer.
          val payload = "{\"error\":\"Runtime replaced\"}"
          response.setBodyText(payload)
          response.setStatus(503)
          response.addHeader("Content-Length", payload.toByteArray(Charsets.UTF_8).size.toString())
          response.addHeader("Content-Type", "application/json")
          return response
        }
        // Client went away. Don't bother writing a body — the socket is
        // already closed. Clean up tracking and let the lambda return.
        return response
      }
      if (res.bodyBytes != null) {
        // setBodyData writes bytes into a Netty ByteBuf and sets Content-Type
        // internally; Content-Length is computed from the buffer at response
        // build time, so we must NOT addHeader("Content-Length", ...) here.
        response.setBodyData(res.contentType, res.bodyBytes)
        response.setStatus(res.statusCode)
      } else {
        val text = res.bodyText ?: ""
        response.setBodyText(text)
        response.setStatus(res.statusCode)
        // UTF-8 byte length, not String.length (grapheme count) — needed so
        // the advertised Content-Length matches the bytes Netty actually writes.
        response.addHeader("Content-Length", text.toByteArray(Charsets.UTF_8).size.toString())
        response.addHeader("Content-Type", res.contentType)
      }
      for ((key, value) in res.headers) {
        response.addHeader(key, value)
      }
      responses.remove(requestUuid)
      cancelled.remove(requestUuid)
      return response
    }

    /**
     * Build a fresh AndroidServer bound to `port` with a single wildcard
     * catch-all per HTTP method. Every request is forwarded to JS via
     * `onRequest`; JS owns method+path routing. safframework's PathTrie uses
     * `*` for single-segment wildcards — matches our flat paths.
     */
    private fun buildServerWithCatchAll(port: Int): Server {
      var s: Server = AndroidServer.Builder {
        port {
          port
        }
      }.build()
      val methods = listOf(
        HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT,
        HttpMethod.DELETE, HttpMethod.OPTIONS,
      )
      for (method in methods) {
        s = s.request(method, "/*", ::dispatchToJs)
      }
      return s
    }

    /**
     * Close the server AND wait (bounded) for the underlying Netty channel to
     * finish unbinding before returning. safframework's `Server.close()` returns
     * void and discards the close future, so we grab the server channel via
     * reflection and await it ourselves. Without this, a back-to-back rebind on
     * the same port races the socket teardown → EADDRINUSE on dev reload.
     *
     * Preserves `listeningPort` because callers (lifecycle hooks, ensureListening
     * on rebind) need the prior port. Explicit `stop()` sets userStopped, so
     * the foreground observer won't auto-rebind.
     */
    private fun closeServerAwait() {
      val s = server ?: return
      val channel = extractServerChannel(s)
      try {
        s.close()
      } catch (e: Throwable) {
        // Closing an already-closed server may throw — swallow.
      }
      try {
        channel?.closeFuture()?.await(500, TimeUnit.MILLISECONDS)
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
      } catch (e: Throwable) {
        // Reflection miss or unexpected close-future failure — fall through.
      }
      server = null
    }

    /**
     * safframework's HttpResponse keeps the Netty Channel in a private `channel`
     * field with no accessor. Reflection reads it so we can listen to socket
     * close events. Returns null if the field layout ever changes (in which
     * case cancellation silently degrades to the staleness-TTL fallback).
     */
    private fun extractChannel(response: Response): io.netty.channel.Channel? {
      return try {
        val field = response.javaClass.getDeclaredField("channel")
        field.isAccessible = true
        field.get(response) as? io.netty.channel.Channel
      } catch (e: Throwable) {
        null
      }
    }

    /**
     * Pull the bound server-channel out of safframework's AndroidServer via
     * reflection so we can `await` its `closeFuture` during teardown. Walks
     * the class hierarchy because the channel field may live on a superclass.
     * Returns null if no `Channel`-typed field is found — caller degrades to
     * best-effort close.
     */
    private fun extractServerChannel(server: Server): io.netty.channel.Channel? {
      var cls: Class<*>? = server.javaClass
      while (cls != null) {
        for (field in cls.declaredFields) {
          if (io.netty.channel.Channel::class.java.isAssignableFrom(field.type)) {
            try {
              field.isAccessible = true
              val value = field.get(server)
              if (value is io.netty.channel.Channel) return value
            } catch (e: Throwable) {
              // Try the next field.
            }
          }
        }
        cls = cls.superclass
      }
      return null
    }
  }

  private val isActive: Boolean get() = activeModule.get() === this

  override fun definition() = ModuleDefinition {

    Name("ExpoHttpServer")

    Events("onStatusUpdate", "onRequest", "onRequestCancel")

    OnCreate {
      // Newest context wins: in-flight requests and every subsequent one go to
      // the runtime that can actually answer them.
      activeModule = WeakReference(this@ExpoHttpServerModule)
      generation.incrementAndGet()
    }

    OnDestroy {
      // A reload has already installed a newer module as the active one, and
      // the shared socket belongs to it now — closing it here is exactly the
      // race this module used to lose. Only the last context standing tears
      // the server down.
      if (activeModule.get() != null && !isActive) return@OnDestroy
      activeModule = WeakReference(null)
      generation.incrementAndGet()
      synchronized(bindLock) {
        closeServerAwait()
        listeningPort = null
      }
    }

    // Background: tear the server down and emit PAUSED. Netty's NIO loop
    // keeps holding the socket while the process is alive otherwise — and
    // doing the explicit close on background gives us a clean rebind on
    // foreground regardless of whether Android decides to kill the process.
    OnActivityEntersBackground {
      if (!isActive) return@OnActivityEntersBackground
      synchronized(bindLock) {
        if (userStopped || listeningPort == null || server == null) return@synchronized
        closeServerAwait()
        sendEvent("onStatusUpdate", bundleOf(
          "status" to "PAUSED",
          "message" to "Server paused"
        ))
      }
    }

    // Foreground: re-bind to the prior port if we have one and the user
    // didn't explicitly stop us.
    OnActivityEntersForeground {
      if (!isActive) return@OnActivityEntersForeground
      synchronized(bindLock) {
        if (userStopped || server != null) return@synchronized
        val port = listeningPort ?: return@synchronized
        if (!bindWithRetry(
              port,
              successStatus = "RESUMED",
              successMessage = "Server resumed",
              attempts = RESUME_BIND_ATTEMPTS
            )) {
          sendEvent("onStatusUpdate", bundleOf(
            "status" to "ERROR",
            "message" to "Failed to rebind HTTP server to port $port"
          ))
        }
      }
    }

    AsyncFunction("ensureListening") { port: Int, promise: Promise ->
      try {
        synchronized(bindLock) {
          userStopped = false
          if (listeningPort == port && server != null) {
            promise.resolve(null)
            return@synchronized
          }
          closeServerAwait()
          if (bindWithRetry(port, successStatus = "STARTED", successMessage = "Server started")) {
            listeningPort = port
            promise.resolve(null)
          } else {
            val msg = "Failed to bind HTTP server to port $port"
            sendEvent("onStatusUpdate", bundleOf(
              "status" to "ERROR",
              "message" to msg
            ))
            promise.reject("ERR_SERVER_START", msg, null)
          }
        }
      } catch (e: Throwable) {
        sendEvent("onStatusUpdate", bundleOf(
          "status" to "ERROR",
          "message" to (e.message ?: "Unknown error starting server")
        ))
        promise.reject("ERR_SERVER_START", e.message ?: "Unknown error", e)
      }
    }

    Function("respond") { uuid: String,
                          statusCode: Int,
                          statusDescription: String,
                          contentType: String,
                          headers: HashMap<String, String>,
                          body: String ->
      responses[uuid] = SimpleHttpResponse(
        statusCode, statusDescription, contentType, headers,
        bodyText = body, bodyBytes = null,
      )
    }

    Function("respondBinary") { uuid: String,
                                statusCode: Int,
                                statusDescription: String,
                                contentType: String,
                                headers: HashMap<String, String>,
                                body: Uint8Array ->
      // Copy the typed-array bytes out of the JS-backed buffer immediately —
      // the Uint8Array is only valid for the lifetime of this Function call,
      // but the route handler lambda may read it later on a different thread.
      val bytes = ByteArray(body.byteLength)
      body.read(bytes, 0, bytes.size)
      responses[uuid] = SimpleHttpResponse(
        statusCode, statusDescription, contentType, headers,
        bodyText = null, bodyBytes = bytes,
      )
    }

    AsyncFunction("stop") { promise: Promise ->
      synchronized(bindLock) {
        userStopped = true
        closeServerAwait()
      }
      sendEvent("onStatusUpdate", bundleOf(
        "status" to "STOPPED",
        "message" to "Server stopped"
      ))
      promise.resolve(null)
    }
  }

  /**
   * Bind on `port`, retrying `attempts - 1` times at `BIND_RETRY_DELAY_MS`
   * spacing. Used by both `ensureListening` (cold start / explicit rebind) and
   * `OnActivityEntersForeground` (resume after backgrounded close) to ride over
   * the window where a prior socket hasn't been released. Emits the success
   * event on success; returns whether bind ultimately succeeded. Caller owns
   * the failure-side status emit so the message can match its context.
   *
   * Call under `bindLock`.
   */
  private fun bindWithRetry(
    port: Int,
    successStatus: String,
    successMessage: String,
    attempts: Int = BIND_ATTEMPTS
  ): Boolean {
    for (attempt in 1..attempts) {
      if (tryBind(port, successStatus, successMessage)) return true
      if (attempt == attempts) break
      try {
        Thread.sleep(BIND_RETRY_DELAY_MS)
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        return false
      }
    }
    return false
  }

  private fun tryBind(port: Int, successStatus: String, successMessage: String): Boolean {
    return try {
      server = buildServerWithCatchAll(port)
      server?.start()
      sendEvent("onStatusUpdate", bundleOf(
        "status" to successStatus,
        "message" to successMessage
      ))
      true
    } catch (e: Throwable) {
      // A half-built server can still hold the socket; close before retrying.
      try {
        server?.close()
      } catch (t: Throwable) {
        // Nothing to salvage.
      }
      server = null
      false
    }
  }
}
