package lab

import org.apache.spark.sql
import org.apache.spark.sql.Row

object RowOps {

  sealed trait ColumnType

  trait IntColumn extends ColumnType

  trait LongColumn extends ColumnType

  trait StringColumn extends ColumnType

  trait BinaryColumn extends ColumnType

  sealed trait ColumnReader[C <: ColumnType] {
    self =>
    type Out

    def read(row: sql.Row)(idx: Int): Out

    def map[Out1](f: Out => Out1): ColumnReader[C] {type Out = Out1} =
      new ColumnReader[C] {
        type Out = Out1

        def read(row: Row)(idx: Int): Out = {
          f(self.read(row)(idx))
        }
      }
  }

  implicit class RowOps(val row: Row) extends AnyVal {
    def read[C <: ColumnType](idx: Int)(implicit reader: ColumnReader[C]): reader.Out = {
      reader.read(row)(idx)
    }
  }

  class IntReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Int

    def read(row: Row)(idx: Int): Out = row.getInt(idx)
  }

  class LongReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Long

    def read(row: Row)(idx: Int): Out = row.getLong(idx)
  }

  class StringReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = String

    def read(row: Row)(idx: Int): Out = row(idx) match {
      case null => ""
      case str: String => str
      case arr: Array[_] => new String(arr.asInstanceOf[Array[Byte]])
    }
  }

  class StringArrayReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Array[String]

    def read(row: Row)(idx: Int): Out = row(idx) match {
      case null => Array.empty[String]
      case arr: Array[_] => arr.map(_.toString)
    }
  }

  class BinaryReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Array[Byte]

    def read(row: Row)(idx: Int): Out = {
      row.getAs[Array[Byte]](idx)
    }
  }

  implicit val intReader = new IntReader[IntColumn]
  implicit val longReader = new LongReader[LongColumn]
  implicit val stringReader = new StringReader[StringColumn]
  implicit val binaryReader = new BinaryReader[BinaryColumn]
}
