package zio.connect.ftp

import zio._
import zio.connect.ftp.FtpConnector.PathName
import zio.ftp.FtpResource
import zio.prelude.Newtype
import zio.stream.{ZSink, ZStream}

import java.io.IOException

trait FtpConnector {

  def stat(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Option[FtpResource]]

  def rm(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit]

  def rmDir(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit]

  def mkDir(implicit trace: Trace): ZSink[Any, IOException, PathName, PathName, Unit]

  def ls(path: => PathName)(implicit trace: Trace): ZStream[Any, IOException, FtpResource]

  def lsDescendant(path: => String): ZStream[Any, IOException, FtpResource]

  def readFile(path: => String, chunkSize: => Int = 2048)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def upload(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

}

object FtpConnector {

  object PathName extends Newtype[String]
  type PathName = PathName.Type

}
