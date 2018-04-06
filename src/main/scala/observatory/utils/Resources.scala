package observatory.utils

import java.nio.file.Paths

object Resources {

  def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}
