package edu.itmo.datamart

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategies}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonSupport {
    val mapper: ObjectMapper = new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
        .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)

    def stringify(value: Any): String = mapper.writeValueAsString(value)

    def parse[A](value: String, clazz: Class[A]): A = mapper.readValue(value, clazz)
}