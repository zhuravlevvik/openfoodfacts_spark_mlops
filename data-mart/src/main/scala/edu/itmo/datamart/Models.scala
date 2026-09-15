package edu.itmo.datamart

final case class PreparedProduct(
    code: String,
    productName: String,
    categories: String,
    features: Vector[Double]
)

final case class DatasetMetadata(version: String, count: Int, createdAt: String)

final case class RefreshResponse(version: String, count: Int, createdAt: String)

final case class DatasetPage(
    version: String,
    total: Int,
    offset: Int,
    limit: Int,
    items: Vector[PreparedProduct]
)

final case class Prediction(code: String, cluster: Int)

final case class PublishResultsRequest(
    runId: String,
    createdAt: String,
    silhouette: Double,
    predictions: Vector[Prediction]
)

final case class PublishResultsResponse(runId: String, publishedRows: Int, status: String)

final case class HealthResponse(status: String, component: String)

final case class ErrorResponse(error: String)