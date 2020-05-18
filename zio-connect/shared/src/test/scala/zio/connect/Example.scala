package zio.connect

import zio.connect.s3._ 

object Example {

  getObject("foo", "bar") >>> putObject("baz", "buz")

}