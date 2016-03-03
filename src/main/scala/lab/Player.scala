package lab

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType, _}

case class Player(id: Int, position: String, lastName: String)

object Player {
  self =>
  val id = "id"
  val position = "position"
  val last_name = "last_name"

  object Schema extends SchemaDefinition {
    val id = structField(self.id, IntegerType)
    val position = structField(self.position, StringType)
    val last_name = structField(self.last_name, StringType)
  }

  val schema: StructType = StructType(Schema.fields)

  import RowOps._

  def parse(row: Row): Player = Player(
    row.read[IntColumn](0),
    row.read[StringColumn](1),
    row.read[StringColumn](2)
  )
}
