package zio.connect

import zio._
import zio.connect.ftp.FtpConnector.PathName
import zio.ftp.FtpResource
import zio.stream.{ZSink, ZStream}

import java.io.IOException

package object ftp {

  def stat(implicit trace: Trace): ZSink[FtpConnector, IOException, PathName, PathName, Option[FtpResource]] =
    ZSink.serviceWithSink(_.stat)

  def rm(implicit trace: Trace): ZSink[FtpConnector, IOException, PathName, PathName, Unit] =
    ZSink.serviceWithSink(_.rm)

  def rmDir(implicit trace: Trace): ZSink[FtpConnector, IOException, PathName, PathName, Unit] =
    ZSink.serviceWithSink(_.rmDir)

  def mkDir(implicit trace: Trace): ZSink[FtpConnector, IOException, PathName, PathName, Unit] =
    ZSink.serviceWithSink(_.mkDir)

  def ls(path: => PathName)(implicit trace: Trace): ZStream[FtpConnector, IOException, FtpResource] =
    ZStream.serviceWithStream(_.ls(path))

  def lsDescendant(path: => PathName): ZStream[FtpConnector, IOException, FtpResource] =
    ZStream.serviceWithStream(_.lsDescendant(path))

  def readFile(path: => PathName, chunkSize: Int = 2048)(implicit trace: Trace): ZStream[FtpConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.readFile(path, chunkSize))

  def upload(pathName: => PathName)(implicit trace: Trace): ZSink[Scope & FtpConnector, IOException, Byte, Nothing, Unit] =
    ZSink.serviceWithSink[FtpConnector](_.upload(pathName))

}
