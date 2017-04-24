name := "SparkSBT"

version := "1.0"

scalaVersion := "2.10.4"

net.virtualvoid.sbt.graph.Plugin.graphSettings

//packSettings


mergeStrategy in assembly := { 
      case n if n.startsWith("META-INF/eclipse.inf") => MergeStrategy.discard
        case n if n.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.discard
          case n if n.startsWith("META-INF/ECLIPSE_.RSA") => MergeStrategy.discard
            case n if n.startsWith("META-INF/ECLIPSEF.SF") => MergeStrategy.discard
              case n if n.startsWith("META-INF/ECLIPSE_.SF") => MergeStrategy.discard
                case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
                  case n if n.startsWith("META-INF/NOTICE.txt") => MergeStrategy.discard
                    case n if n.startsWith("META-INF/NOTICE") => MergeStrategy.discard
                      case n if n.startsWith("META-INF/LICENSE.txt") => MergeStrategy.discard
                        case n if n.startsWith("META-INF/LICENSE") => MergeStrategy.discard
                          case n if n.startsWith("rootdoc.txt") => MergeStrategy.discard
                            case n if n.startsWith("readme.html") => MergeStrategy.discard
                              case n if n.startsWith("readme.txt") => MergeStrategy.discard
                                case n if n.startsWith("library.properties") => MergeStrategy.discard
                                  case n if n.startsWith("license.html") => MergeStrategy.discard
                                    case n if n.startsWith("about.html") => MergeStrategy.discard
                                      case _ => MergeStrategy.last
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1"

javaOptions += "-Xmx2G"
