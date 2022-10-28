package zio.connect.ftp

import zio._
import zio.connect.ftp.FtpConnector.PathName
import zio.ftp.{Ftp, FtpResource}
import zio.stream.{ZSink, ZStream}

import java.io.IOException

case class LiveFtpConnector(ftp: Ftp) extends FtpConnector {

  override def stat(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Option[FtpResource]] =
    ZSink
      .take[PathName](1)
      .map(_.headOption)
      .mapZIO {
        case Some(path) => ftp.stat(path)
        case None       => ZIO.succeed(None)
      }

  override def rm(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit] = {
    ZSink
      .foreach[Any, IOException, PathName] { path =>
        ftp.rm(path)
      }
  }

  override def rmDir(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit] = {
    ZSink
      .foreach[Any, IOException, PathName] { path =>
        ftp.rm(path)
      }
  }

  override def mkDir(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit] = {
    ZSink
      .foreach[Any, IOException, PathName] { path =>
        ftp.mkdir(path)
      }
  }

  override def ls(path: => PathName)(implicit trace: Trace): ZStream[Any, IOException, FtpResource] = {
    ftp.ls(path)
  }

  override def lsDescendant(path: => PathName): ZStream[Any, IOException, FtpResource] = {
    ftp.lsDescendant(path)
  }

  override def readFile(path: => PathName, chunkSize: Int = 2048)(implicit trace: Trace): ZStream[Any, IOException, Byte] = {
    ftp.readFile(path, chunkSize)
  }

  override def upload[R](pathName: => PathName)(implicit trace: Trace): ZSink[R & Scope, IOException, Byte, Nothing, Unit] = {
    ZSink
      .foreachChunk[R & Scope, IOException, Byte] { content =>
        ftp.upload(
          path = pathName,
          source = ZStream.fromChunk(content)
        )
      }
  }
}

object LiveFtpConnector {

  val layer: ZLayer[Ftp, Nothing, LiveFtpConnector] =
    ZLayer.fromZIO(ZIO.service[Ftp].map(LiveFtpConnector(_)))

}