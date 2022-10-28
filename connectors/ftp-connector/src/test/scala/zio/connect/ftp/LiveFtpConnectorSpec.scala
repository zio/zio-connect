package zio.connect.ftp

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import zio._
import zio.ftp._

object LiveFtpConnectorSpec extends FtpConnectorSpec {

  override def spec =
    suite("LiveFtpConnectorSpec")(ftpConnectorSpec)
      .provideSomeShared[Scope](
        ftpContainer,
        ftpLayer,
        zio.connect.ftp.ftpConnectorLiveLayer
      )

  val ftpContainer: ZLayer[Scope, Throwable, FixedHostPortGenericContainer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val ftp = FixedHostPortGenericContainer(
          "stilliard/pure-ftpd:latest",
          exposedPorts = Seq(21),
          env = Map(
            "FTP_USER_NAME" -> "username",
            "FTP_USER_PASS" -> "password",
            "FTP_USER_HOME" -> "/home/username"
          ),
          exposedHostPort = 30000,
          exposedContainerPort = 30000
        )
        (1 to 10) foreach { idx =>
          val port = 30000 + idx
          ftp.container.withFixedExposedPort(port, port)
        }
        ftp.start()
        ftp
      })(f => ZIO.attempt(f.stop()).orDie)
    )
        //        ftp.withCreateContainerCmdModifier(e => {
        //          val bindings = (0 until 10) map { idx =>
        //            val port = 30000 + idx
        //            new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))
        //          }
        //          e.withPortSpecs()
        //        })



lazy val ftpLayer: ZLayer[Scope & FixedHostPortGenericContainer, ConnectionError, Ftp] =
  ZLayer
    .fromZIO (
      for {
        containers <- ZIO.service[FixedHostPortGenericContainer]
        _ <- ZIO.log (s"FTP >>> ${containers.container.getHost} ${containers.container.getMappedPort(21)}")
        ftp <- ZIO.succeed(unsecure(UnsecureFtpSettings(containers.container.getHost, containers.container.getMappedPort(21), FtpCredentials ("username", "password"))))
      } yield ftp
    )
    .flatMap (_.get)

  }
