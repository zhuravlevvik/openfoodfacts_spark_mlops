package edu.itmo.datamart

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Executors}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

final class DataMartHttpService(config: ServiceConfig, store: DatasetStore) {
    private val server = HttpServer.create(new InetSocketAddress(config.host, config.port), 0)
    server.createContext("/", new RootHandler)
    server.setExecutor(Executors.newFixedThreadPool(8))

    private def query(exchange: HttpExchange, name: String, default: Int): Int =
        Option(exchange.getRequestURI.getRawQuery)
            .toVector
            .flatMap(_.split("&"))
            .map(_.split("=", 2))
            .collectFirst { case Array(key, value) if key == name => value.toInt  }
            .getOrElse(default)

    private def requestBody(exchange: HttpExchange): String =
        new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
    
    private def respond(exchange: HttpExchange, status: Int, value: Any): Unit = {
        val bytes = JsonSupport.stringify(value).getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
        exchange.sendResponseHeaders(status, bytes.length.toLong)
        val body = exchange.getResponseBody
        try body.write(bytes)
        finally body.close()
    }

    private final class RootHandler extends HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
            try route(exchange)
            catch {
                case error: IllegalArgumentException => respond(exchange, 400, ErrorResponse(error.getMessage))
                case error: IllegalStateException    => respond(exchange, 409, ErrorResponse(error.getMessage))
                case error: Exception                => respond(exchange, 500, ErrorResponse(error.getMessage))
            } finally exchange.close()
        }

        private def route(exchange: HttpExchange): Unit = {
            val method = exchange.getRequestMethod
            val path = exchange.getRequestURI.getPath
            (method, path) match {
                case ("GET", "/live") =>
                    respond(exchange, 200, HealthResponse("ok", "openfoodfacts-data-mart"))
                case ("GET", "/health") =>
                    if (!store.hasRawProducts()) {
                        throw new IllegalStateException("source dataset has not been seeded yet")
                    }
                    respond(exchange, 200, HealthResponse("ok", "openfoodfacts-data-mart"))
                case ("POST", "/v1/datasets/refresh") =>
                    val offset = query(exchange, "offset", 0)
                    val limit = query(exchange, "limit", config.pageLimit)
                    respond(exchange, 200, store.currentPage(offset, limit))
                case ("POST", "/v1/results") =>
                    val request = JsonSupport.parse(requestBody(exchange), classOf[PublishResultsRequest])
                    respond(exchange, 200, store.publishResults(request))
                case _ => respond(exchange, 404, ErrorResponse(s"route not found: $method $path"))
            }
        }
    }

    def runUntilShutdown(): Unit = {
        val stopped = new CountDownLatch(1)
        Runtime.getRuntime.addShutdownHook(new Thread(() => {
            server.stop(5)
            stopped.countDown()
        }))
        server.start()
        println(s"Data mart listens on ${config.host}:${config.port}")
        stopped.await()
    }
}
