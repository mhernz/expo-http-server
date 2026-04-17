package expo.modules.httpserver

import androidx.core.os.bundleOf
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import expo.modules.kotlin.typedarray.Uint8Array
import com.safframework.server.core.AndroidServer
import com.safframework.server.core.Server
import com.safframework.server.core.http.HttpMethod
import com.safframework.server.core.http.Request
import com.safframework.server.core.http.Response
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class ExpoHttpServerModule : Module() {
  class SimpleHttpResponse(val statusCode: Int,
                           val statusDescription: String,
                           val contentType: String,
                           val headers: HashMap<String, String>,
                           val bodyText: String?,
                           val bodyBytes: ByteArray?)

  private var server: Server? = null;
  private var started = false;
  // ConcurrentHashMap because route lambdas run on Netty worker threads while
  // respondBinary/respond run on the JS thread — both touch this map.
  private val responses = ConcurrentHashMap<String, SimpleHttpResponse>()
  // Per-request cancellation flag. Set when the underlying Netty channel
  // closes (client disconnect) so the busy-wait loop can exit instead of
  // pinning a worker thread for tiles no one is waiting for.
  private val cancelled = ConcurrentHashMap<String, Boolean>()

  override fun definition() = ModuleDefinition {

    Name("ExpoHttpServer")

    Events("onStatusUpdate", "onRequest", "onRequestCancel")

    Function("setup") { port: Int ->
      server = AndroidServer.Builder{
        port {
          port
        }
      }.build()
    }

    Function("route") { path: String, method: String, uuid: String ->
      server = server?.request(HttpMethod.getMethod(method), path) { request: Request, response: Response ->
        // Per-request uuid: without this, 16 concurrent tile requests all
        // busy-wait on responses[<route-uuid>] and whichever respondBinary
        // writes last wins — bytes get delivered to the wrong connection.
        val requestUuid = UUID.randomUUID().toString()

        // Register a close listener on the underlying Netty channel so that
        // MapLibre-native cancelling a tile (socket close before we respond)
        // propagates to JS as `onRequestCancel`. safframework's HttpResponse
        // stores the Channel in a private field, hence reflection — we pin
        // AndroidServer v1.3.3 in build.gradle so the field is stable.
        val channel = extractChannel(response)
        channel?.closeFuture()?.addListener {
          if (responses.containsKey(requestUuid) || cancelled.putIfAbsent(requestUuid, true) == null) {
            cancelled[requestUuid] = true
            sendEvent("onRequestCancel", bundleOf("uuid" to requestUuid))
          }
        }

        val headers: Map<String, String> = request.headers()
        val params: Map<String, String> = request.params()
        val cookies: Map<String, String> = request.cookies().associate { it.name() to it.value() }
        sendEvent("onRequest", bundleOf(
          "uuid" to requestUuid,
          "routeUuid" to uuid,
          "method" to request.method().name,
          "path" to request.url(),
          "body" to request.content(),
          "headersJson" to JSONObject(headers).toString(),
          "paramsJson" to JSONObject(params).toString(),
          "cookiesJson" to JSONObject(cookies).toString(),
        ))
        // Wait for JS to produce a response, OR for the client to disconnect.
        // Without the cancellation check, abandoned requests would pin this
        // Netty worker thread indefinitely.
        while (!responses.containsKey(requestUuid) && cancelled[requestUuid] != true) {
          Thread.sleep(10)
        }
        val res = responses[requestUuid]
        if (res == null) {
          // Client went away. Don't bother writing a body — the socket is
          // already closed. Clean up tracking and let the lambda return.
          cancelled.remove(requestUuid)
          // Returning the response object as-is; Netty will skip writing to
          // a closed channel.
          return@request response
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
        responses.remove(requestUuid);
        cancelled.remove(requestUuid);
        return@request response
      };
    }

    Function("start") {
      if (server == null) {
        sendEvent("onStatusUpdate", bundleOf(
          "status" to "ERROR",
          "message" to "Server not setup / port not configured"
        ))
      } else {
        if (!started) {
          started = true
          server?.start()
          sendEvent("onStatusUpdate", bundleOf(
            "status" to "STARTED",
            "message" to "Server started"
          ))
        }
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

    Function("stop") {
      started = false
      server?.close()
      sendEvent("onStatusUpdate", bundleOf(
        "status" to "STOPPED",
        "message" to "Server stopped"
      ))
    }
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
}
