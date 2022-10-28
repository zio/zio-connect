package zio.connect.ftp

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
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

  val ftpContainer: ZLayer[Scope, Throwable, GenericContainer[_]] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val ftpImage = DockerImageName.parse("delfer/alpine-ftp-server:latest")
        val ftp = new GenericContainer(ftpImage)
        ftp.withExposedPorts(21)
        ftp.withEnv("USERS", "usermame|password")
        ftp.start()
        ftp
      })(f => ZIO.attempt(f.stop()).orDie)
    )

  lazy val ftpLayer: ZLayer[Scope & GenericContainer[_], ConnectionError, Ftp] =
    ZLayer
      .fromZIO(
        for {
          containers <- ZIO.service[GenericContainer[_]]
          _          <- ZIO.log(s"FTP >>> ${containers.getHost}  ${containers.getFirstMappedPort}")
          ftp        <- ZIO.succeed(
                    unsecure(UnsecureFtpSettings(containers.getHost, containers.getFirstMappedPort, FtpCredentials("username", "password")))
          )
        } yield ftp
      )
      .flatMap(_.get)

}
