package zio.connect.s3
import zio.Scope
import zio.test.{Spec, TestEnvironment}

object TestS3ConnectorSpec extends S3ConnectorSpec {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("TestS3ConnectorSpec")(s3ConnectorSpec).provide(s3ConnectorTestLayer)

}
