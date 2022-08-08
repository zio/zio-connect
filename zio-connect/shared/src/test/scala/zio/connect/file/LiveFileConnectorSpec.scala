package zio.connect.file

import zio.nio.file.{ Files, Path }
import zio.{ Cause, Scope, ZIO }
import zio.stream.ZStream
import zio.test.Assertion.{ equalTo, failsCause, failsWithA }
import zio.test.{ assertZIO, ZIOSpecDefault }

import java.io.IOException
import java.util.UUID

object LiveFileConnectorSpec extends ZIOSpecDefault {

  val target = LiveFileConnector()

  override def spec =
    suite("LiveFileConnectorSpec")(
      suite("writeFile")(
        test("fails when IOException") {
          val s = target.writeFile(Path(""))
          assertZIO(ZStream("1".toByte).run(s).exit)(failsWithA[IOException])
        },
        test("dies when not IOException") {
          object NonIOException extends Throwable
          val prog = tempFile.flatMap { path =>
            val s = target.writeFile(path).mapZIO(_ => ZIO.fail(NonIOException))
            ZStream("1".toByte).mapZIO(_ => ZIO.fail(NonIOException)).run(s).exit
          }
          assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
        }
      )
    )

  val tempFile: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(Files.createTempFile(UUID.randomUUID().toString, None, List.empty))(f => Files.delete(f).orDie)
}
