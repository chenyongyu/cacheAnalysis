package sparkapps

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * Created by yongyuc on 1/20/17.
 */
object SparkApp1 {
  case class User(
                 Ip:String,
                 BegTime:String
                 )

  def userToString(user: User): String = {
    return user.Ip+"@"+user.BegTime
  }

  trait BasicInfo {
    def Ip: String
    def BegTime: String
    def Protocal: Int
    def Content: Int
    def UserIp: String
    def UserIpv6: String
    def UL: Int
    def DL: Int
    def IsSuccess: Boolean
  }
  case class HTTP(
                      Head:BasicInfo,
                      Host:String,
                      Content:Int,
                      HTTPContentType:String,
                      FirstLatency:Int,
                      Latency:Int,
                      IsCacheAble: Boolean
                      )
  def main(args: Array[String]) {

    val defaultParams = Models.Params()
    val buildInfo = Models.BuildInfo()

    val parser = new OptionParser[Models.Params]("Streaming") {

      head(s"""${buildInfo.name} with Apache Kafka v${buildInfo.version} by ${buildInfo.organization}
               ${buildInfo.author}, ${buildInfo.mail}""")
      opt[String]("zookeeper")
        .valueName("<hostname:port>,<hostname:port>,...")
        .text(s"zookeeper quorum, default: ${defaultParams.zk_quorum}")
        .action((x, c) => c.copy(zk_quorum = x))
      opt[String]("consumer_group")
        .text(s"kafka consumer group name, default: ${defaultParams.consumer_group}")
        .action((x, c) => c.copy(consumer_group = x))
      opt[Seq[String]]('t', "topics")
        .required()
        .valueName("<topic1>,<topic2>,...")
        .text("kafka topics to consume from")
        .action((x, c) => c.copy(topics = x))
      opt[String]("zk_conn_to_ms")
        .text(s"kafka, zookeeper connection timeout in millisecond, default: ${defaultParams.zk_conn_to_ms}")
        .action((x, c) => c.copy(zk_conn_to_ms = x))
      opt[Int]("consumer_thread")
        .text(s"consumer thread number, default: ${defaultParams.consumer_thread}")
        .action((x, c) => c.copy(consumer_thread = x))
      opt[Int]("batch_interval")
        .text(s"batch interval in second, default: ${defaultParams.batch_interval}")
        .action((x, c) => c.copy(batch_interval = x))
      opt[String]("ckp_dir")
        .valueName("<dir>")
        .text("spark checkpoint directory, default: no checkpoint directory")
        .action((x, c) => c.copy(ckp_dir = x))
      opt[String]("log_dir")
        .valueName("<dir>")
        .text("spark event log directory, default: no log directory")
        .action((x, c) => c.copy(log_dir = x))
      opt[String]("cassandra_host")
        .valueName("<host>")
        .text("cassandra host, default: no data saving if this option is not provided")
        .action((x, c) => c.copy(cassandra_host = x))
      opt[String]("cassandra_username")
        .valueName("<user>")
        .text("cassandra user name, default: cassandra")
        .action((x, c) => c.copy(cassandra_username = x))
      opt[String]("cassandra_password")
        .valueName("password").text("cassandra password, default: cassandra")
        .action((x, c) => c.copy(cassandra_password = x))
      opt[Unit]("verbose")
        .text(s"more verbose option, default: ${defaultParams.verbose}")
        .action((_, c) => c.copy(verbose = true))
      note(
        """
          |For example, the following command runs this app, with a 5 seconds batch interval
          |
          | spark-submit --class LinkerStreaming /your_project_path/target/scala-*/main_streaming_linker.jar \
          |  --zookeeper localhost:2181 --consumer_group linker-streaming --batch_interval 5 --cassandra_host localhost\
          |  --cassandra_username username --cassandra_password \
          |  --topics linker_streaming,log_streaming --verbose
        """.stripMargin)
      help("help")
        .text("prints this usage text")
    }
    parser.parse(args, defaultParams).map {
      params => run(params)
    } getOrElse {
      System.exit(1)
    }
  }
  def run(params: Models.Params): Unit = {

    val zk_quorum = params.zk_quorum
    val consumer_group = params.consumer_group
    val topics = params.topics
    val zk_conn_to_ms = params.zk_conn_to_ms
    val consumer_thread = params.consumer_thread
    val batch_interval = params.batch_interval
    val ckp_dir = params.ckp_dir
    val log_dir = params.log_dir
    val cassandra_host = params.cassandra_host
    val cassandra_username = params.cassandra_username
    val cassandra_password = params.cassandra_password
    val verbose = params.verbose

    if (verbose) println("spark streaming context configuring...")

    // init spark conf
    val conf = new SparkConf().setAppName("LinkerStreaming")
      .set("spark.cassandra.connection.host", cassandra_host)
      .set("spark.cassandra.auth.username", cassandra_username)
      .set("spark.cassandra.auth.password", cassandra_password)
      .set("spark.cassandra.connection.timeout_ms", "10000")
    if (log_dir != "") conf.set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", log_dir)
    var ckp_dir_ = if (ckp_dir == "") "/tmp/ckp" else ckp_dir
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batch_interval))
    ssc.checkpoint(ckp_dir_)
    sc.setCheckpointDir(ckp_dir_)

    if (verbose) {
      println("spark streaming context configured")
      println("cassandra initiating...")
    }

    // init cassandra keyspace and tables
    if (cassandra_host != "") {
      CassandraConnector(conf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS raw WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS raw.Info （Ip text,BegTime timestamp, Protocal int, Content int, UserIp text, UserIpv6 text, UL int, DL int, IsSuccess bool, Host text, Content int, HTTPContentType text, FirstLatency int, Latency int, IsCacheAble bool, primary key((Ip, BegTime), Host)))")
        //session.execute("CREATE TABLE IF NOT EXISTS raw.mem_usage_v1 (ts timestamp, date text, machine_id text, mem_usage float, primary key((machine_id, date), ts))")}
      }
    }

    if (verbose) println("cassandra initiated")

    val topic_map = topics.map((_, consumer_thread)).toMap
    val kafka_conf = Map(
      "zookeeper.connect" -> zk_quorum,
      "group.id" -> consumer_group,
      "zookeeper.connection.timeout.ms" -> zk_conn_to_ms
    )

    // get kafka streaming data
    val linkerStreaming = KafkaUtils.createStream(ssc, zk_quorum, consumer_group, topic_map)
    linkerStreaming.filter(con => {
      val lis = con._2.split("|")
      if (lis.:\(15).==(0) && lis.:\(36).==(0) && lis.:\(37).==(1)) {
        return false
      }
      return true
    }).mapPartitions(it_line => {
      it_line.map(con => {
        val lis = con._2.split("|")
        if (lis.:\(8) == 15) {
          if (lis.:\(10) == 2 || lis.:\(10) == 3 || lis.:\(10) == 4) {
            (userToString(User(lis.:\(12).toString().toString(),lis.:\(5).toString())), Cacheable(lis.:\(5).toString(),lis.:\(7),lis.:\(10), lis.:\(12).toString(),lis.:\(13).toString(),lis.:\(19),lis.:\(20),true))
          } else if (lis.:\(10) == 5 || lis.:\(10) == 1) {
            (userToString(User(lis.:\(12).toString().toString(),lis.:\(5).toString())), Cacheable(lis.:\(5).toString(),lis.:\(7),lis.:\(10), lis.:\(12).toString(),lis.:\(13).toString(),lis.:\(19),lis.:\(20),false))
          }
        } else {
          (userToString(User(lis.:\(12).toString().toString(),lis.:\(5).toString())), Cacheable(lis.:\(5).toString(),lis.:\(7),lis.:\(10), lis.:\(12).toString(),lis.:\(13).toString(),lis.:\(19),lis.:\(20),false))
        }
      })
    }).saveToCassandra("raw", "info", SomeColumns("BegTime", "Protocal", "Content", "UserIp", "UserIpv6", "UL", "DL", "IsSuccess", "Host", "Content", "HTTPContentType", "FirstLatency", "Latency", "IsCacheAble"))
  }
}
