package zio.connect.cassandra

import zio.ZIO
import zio.connect.cassandra.CassandraConnector.{CassandraException, CreateKeySpaceObject}
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{Spec, ZIOSpecDefault, assert, assertTrue}

trait CassandraConnectorSpec extends ZIOSpecDefault {

  val cassandraConnectorSpec: Spec[CassandraConnector, CassandraException] =
    createKeySpaceSuite + deleteKeyspaceSuite

  private lazy val createKeySpaceSuite =
    suite("createKeyspace")(
      test("succeed in creating a keyspace") {
        for {
          created <- (ZStream.succeed(CreateKeySpaceObject("keyspace1", None, None)) >>> createKeyspace)
                       .as(true)
                       .catchSome { case _: CassandraException =>
                         ZIO.succeed(false)
                       }
        } yield assertTrue(created)
      },
      test("fail in creating an already existing keyspace") {
        for {
          firstCreated <- (ZStream.succeed(CreateKeySpaceObject("keyspace2", None, None)) >>> createKeyspace)
                            .as(true)
                            .catchSome { case _: CassandraException =>
                              ZIO.succeed(false)
                            }
          secondCreated <- (ZStream.succeed(CreateKeySpaceObject("keyspace2", None, None)) >>> createKeyspace)
                             .as(true)
                             .catchSome { case _: CassandraException =>
                               ZIO.succeed(false)
                             }
        } yield assertTrue(firstCreated) && assert(secondCreated)(equalTo(false))
      }
    )

  private lazy val deleteKeyspaceSuite =
    suite("deleteKeyspace")(
      test("fail when keyspace does not exist") {
        for {
          deleted <- (ZStream.succeed("randomKeyspaceName") >>> deleteKeyspace).as(true).catchSome {
                       case _: CassandraException =>
                         ZIO.succeed(false)
                     }
        } yield assert(deleted)(equalTo(false))
      },
      test("succeed when keyspace exists") {
        for {
          created <- (ZStream.succeed(CreateKeySpaceObject("randomKeyspaceName", None, None)) >>> createKeyspace)
                       .as(true)
                       .catchSome { case _: CassandraException =>
                         ZIO.succeed(false)
                       }
          deleted <- (ZStream.succeed("randomKeyspaceName") >>> deleteKeyspace).as(true).catchSome {
                       case _: CassandraException =>
                         ZIO.succeed(false)
                     }
        } yield assertTrue(created) && assertTrue(deleted)
      }
    )
}
