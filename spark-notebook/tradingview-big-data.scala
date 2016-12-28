
object Cells {
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.mllib.rdd.RDDFunctions._
  import scala.util.{Try, Success, Failure}
  import java.sql.Timestamp
  
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  /* ... new cell ... */

  val btcIdeas = sparkContext.textFile("data/1000_1_BTC_ideas.json")
    .union(sparkContext.textFile("data/1000_2_BTC_ideas.json"))
    .union(sparkContext.textFile("data/1000_3_BTC_ideas.json"))
    .union(sparkContext.textFile("data/1000_4_BTC_ideas.json"))

  /* ... new cell ... */

  val times = btcIdeas
    .flatMap(_.split("data-timestamp"))
    .map(_.split("\\\"")(1))
    .map(x => x.substring(0, x.length - 1))
    .flatMap(x => Try(java.lang.Double.parseDouble(x)).toOption)

  /* ... new cell ... */

  ScatterChart(times.map(x => (x,x)).collect, maxPoints=6000)

  /* ... new cell ... */

  val t = Seq(times.collect.min, times.collect.max)
    .map(x => new Timestamp(x.toLong * 1000));

  /* ... new cell ... */

  val btcNews = sparkContext.textFile("data/yahoo.txt")

  /* ... new cell ... */

  val yahooTimes = sparkContext.textFile("data/yahoo_timestamps.txt")
    .flatMap(_.split("\n"))
    .map(_.toDouble)
    .collect

  /* ... new cell ... */

  ScatterChart(yahooTimes.map(x => (x,x)))

  /* ... new cell ... */

  val t = Seq(yahooTimes.min, yahooTimes.max)
    .map(x => new Timestamp(x.toLong));

  /* ... new cell ... */

  val newsTimes = yahooTimes.sorted.map(_.toDouble)
  val ideasTimes = times.map(_ * 1e3).collect.sorted.map(_.toDouble)

  /* ... new cell ... */

  val both = for {
    newsPair <- newsTimes.sliding(2)
    ti <- ideasTimes if ti >= newsPair(0) && ti < newsPair(1)
  } yield (ti, newsPair(1))
  // select only 22% to be able to plot scatter chart
  ScatterChart(both.filter(x => scala.math.random > 0.78).toSeq)

  /* ... new cell ... */

  import org.apache.spark.mllib.stat.KernelDensity
  import org.apache.spark.rdd.RDD
  
  def myKernelDensity(arr: Array[Double]): KernelDensity = {
    val data = sc.parallelize(arr)
    new KernelDensity()
      .setSample(data)
      .setBandwidth(3e8)
  }
  
  val kdi = myKernelDensity(ideasTimes)
  val kdn = myKernelDensity(newsTimes)
  
  // Find density estimates for the given values
  // 14100 to 14800 * 1e8 - samples in time
  val densitiesi = kdi.estimate((13800 to 14800).map(_ * 1e8).toArray)
  val densitiesn = kdn.estimate((13800 to 14800).map(_ * 1e8).toArray)

  /* ... new cell ... */

  val newsAndIdeasForChart = (densitiesn zip densitiesi)
    .zipWithIndex
    .flatMap{ case ((x,y), i) => Seq((i, x, "news"), (i, y, "ideas")) }
  
  ScatterChart(newsAndIdeasForChart, 
               fields=Some(("_1", "_2")),
               groupField=Some("_3"),
               maxPoints=3000
              )

  /* ... new cell ... */

  (densitiesn.sum * 1e8, densitiesi.sum * 1e8)

  /* ... new cell ... */

  ScatterChart((densitiesi zip densitiesn).filter(_._2 >= 1e-13) drop 0)

  /* ... new cell ... */

  import org.apache.spark.mllib.linalg._
  import org.apache.spark.mllib.stat.Statistics

  /* ... new cell ... */

  val correlation: Double = Statistics.corr(
    sc.parallelize(densitiesi),
    sc.parallelize(densitiesn),
    "spearman"
  )

  /* ... new cell ... */

  val correlation: Double = Statistics.corr(
    sc.parallelize(densitiesi drop 200),
    sc.parallelize(densitiesn drop 200),
    "spearman"
  )

  /* ... new cell ... */

  val correlation: Double = Statistics.corr(
    sc.parallelize(densitiesi drop 400),
    sc.parallelize(densitiesn drop 400),
    "spearman"
  )

  /* ... new cell ... */

  val correlations = for (i <- 1 to 850) yield Statistics.corr(
      sc.parallelize(densitiesi drop i),
      sc.parallelize(densitiesn drop i),
      "spearman"
    )

  /* ... new cell ... */

  ScatterChart(correlations.toArray)

  /* ... new cell ... */

  val correlationsBad = for (i <- 1 to 500) yield Statistics.corr(
      sc.parallelize(densitiesi drop i),
      sc.parallelize(scala.util.Random.shuffle(densitiesi drop i)),
      "spearman"
    )

  /* ... new cell ... */

  ScatterChart(correlationsBad.toArray)

  /* ... new cell ... */

  def yahooListUrl(year: Int, month: Any, day:String = "01") = s"https://uk.finance.yahoo.com/q/h?s=AAPL&t=$year-$month-${day}T00:00:00+01:00"
  yahooListUrl(2014, 5, "01")

  /* ... new cell ... */

  val yahooUrls = for {
    year <- 2014 to 2016
    month <- Seq("01","02","03","04","05","06","07","08","09","10","11","12")
    day <- Seq("01", "07", "15", "22")
  } yield yahooListUrl(year, month, day)

  /* ... new cell ... */

  val aaplNewsUrls = sc.parallelize(yahooUrls)
    .flatMap(x => Try{
      val s = scala.io.Source.fromURL(x, "utf-8")
      val res = s.getLines.flatMap(_.split("<li>")).filter(_.contains("https://uk.finance.yahoo.com/news")).toList
      s.close()
      res
    }.toOption)
    .flatMap(x => x)
    .flatMap(x => "href=\"(https://uk.finance.yahoo.com/news/(.*?).html)\"".r.findFirstMatchIn(x).map(_.group(1)))
    .distinct

  /* ... new cell ... */

  aaplNewsUrls.collect.length

  /* ... new cell ... */

  val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val yahooTimes = aaplNewsUrls
    .flatMap(x => Try{
      val s = scala.io.Source.fromURL(x, "utf-8")
      val res = s.getLines.filter(_.contains("h1 class=\"headline\"")).take(1).toList
      s.close()
      res
    }.toOption)
    .flatMap(x => x)
    .flatMap(x => "abbr title=\"(.*?)\"".r.findFirstMatchIn(x).map(_.group(1)))
    .map(dateFormatter.parse(_))
    .map(_.getTime)
    .collect

  /* ... new cell ... */

  val aaplYahooTimes = yahooTimes

  /* ... new cell ... */

  yahooTimes.length

  /* ... new cell ... */

  val aaplIdeas = sparkContext.textFile("data/1000_1_AAPL_ideas.json")
    .union(sparkContext.textFile("data/1000_2_AAPL_ideas.json"))
    .union(sparkContext.textFile("data/1000_3_AAPL_ideas.json"))
    .union(sparkContext.textFile("data/1000_4_AAPL_ideas.json"))
    .union(sparkContext.textFile("data/1000_5_AAPL_ideas.json"))
    .union(sparkContext.textFile("data/1000_6_AAPL_ideas.json"))

  /* ... new cell ... */

  val times = aaplIdeas
    .flatMap(_.split("data-timestamp"))
    .map(_.split("\\\"")(1))
    .map(x => x.substring(0, x.length - 1))
    .flatMap(x => Try(java.lang.Double.parseDouble(x)).toOption)

  /* ... new cell ... */
}
                  