package zio.connect.testkit.file

import zio.{Chunk, Ref, ZIO, ZLayer}
import zio.connect.file.FileConnector
import zio.connect.testkit.file.TestFileConnector.{Dir, File, NonDir}
import zio.nio.file.Path
import zio.test.Assertion._
import zio.test._

object TestFileConnectorSpec extends ZIOSpecDefault {

  lazy val root    = Dir(Path("root"))
  lazy val file1   = NonDir(Path("root", "file1"))
  lazy val dir1    = Dir(Path("root", "dir1"))
  lazy val file1_1 = Dir(Path("root", "dir1", "file1_1"))

  override def spec =
    suite("TestFileConnectorSuite")(
      listDirSuite
    ).provide(
      ZLayer.fromZIO(
        for {
          fs <- Ref.make[Map[Path, File]](
                  Map(root.path -> root, file1.path -> file1, dir1.path -> dir1, file1_1.path -> file1_1)
                )
          r <- ZIO.succeed(TestFileConnector(fs))
        } yield r
      )
    )

  private lazy val listDirSuite =
    suite("listDir")(
      test("succeeds") {
        for {
          children <- FileConnector.listDir(root.path).runCollect
        } yield assert(children)(equalTo(Chunk(file1.path, dir1.path)))
      },
      test("fails when IOException") {
        ???
      },
    )
}
