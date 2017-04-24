package sparkapps

/**
  * Created by yongyuc on 4/24/2017.
  */
package object Models {
  case class Params(
                     zk_quorum: String = "192.168.10.130:2181",
                     consumer_group: String = "tmp-consumer-group",
                     topics: Seq[String] = Seq(),
                     zk_conn_to_ms: String = "10000",
                     consumer_thread: Int = 1,
                     batch_interval: Int = 5,
                     ckp_dir: String = "",
                     log_dir: String = "",
                     cassandra_host: String = "",
                     cassandra_username: String = "cassandra",
                     cassandra_password: String = "cassandra",
                     verbose: Boolean = false
                   )
  case class BuildInfo(
                        name: String = "Data Streaming",
                        version: String = "1.0",
                        author: String = "Chen Yongyu",
                        mail: String = "396279682@qq.com",
                        organization: String = "Christian"
                      )
}
