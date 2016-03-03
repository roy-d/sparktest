package lab

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{MustMatchers, FlatSpec}

class ASparkSpec extends FlatSpec with MustMatchers with SparkSupport {

  private val players = Seq(
    Row(1, "GK", "Stegen"),
    Row(2, "DF", "Douglas"),
    Row(3, "DF", "Pique"),
    Row(4, "MF", "Rakitic"),
    Row(5, "MF", "Busquets"),
    Row(6, "DF", "Alves"),
    Row(7, "MF", "Turan"),
    Row(8, "MF", "Iniesta"),
    Row(9, "FW", "Suarez"),
    Row(10, "FW", "Messi"),
    Row(11, "FW", "Neymar"),
    Row(12, "MF", "Rafinha"),
    Row(13, "GK", "Bravo"),
    Row(14, "MF", "Mascherano"),
    Row(15, "DF", "Bartra"),
    Row(17, "FW", "Haddadi"),
    Row(18, "DF", "Alba"),
    Row(19, "FW", "Ramirez"),
    Row(20, "MF", "Roberto"),
    Row(21, "DF", "Adriano"),
    Row(22, "MF", "Vidal"),
    Row(23, "DF", "Vermaelen"),
    Row(24, "DF", "Mathieu"),
    Row(25, "GK", "Masip")
  )

  private val squad: DataFrame = sqlContext.createDataFrame(sc.parallelize(players), Player.schema)

  "Squad" should "allow parsing" in {
    val players = squad.collect().map(Player.parse)
    players.length must === (24)

    val forwards = players.filter(player => player.position == "FW")
    forwards.size must === (5)
  }

  it should "allow sql" in {
    val groupByPosition = squad.groupBy("position").count().collect()
    groupByPosition.size must === (4)
  }

}
