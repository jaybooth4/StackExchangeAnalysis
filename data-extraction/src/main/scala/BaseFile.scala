package XMLParse

import java.io.File
import java.text.SimpleDateFormat
import java.util.TimeZone

// Base file that holds functions used across the types of files that we are reading in
abstract class BaseFile {

  // Function to get path of file resources
  private[XMLParse] def FilePath(fileName: String) = {
    val resource = this.getClass.getClassLoader.getResource("FinalProject/" + fileName + ".xml")
    if (resource == null) sys.error("Could not locate file")
    new File(resource.toURI).getPath
  }

  private[XMLParse] def parseOptionInt(int: scala.xml.NodeSeq): Option[Int] =
    int.text match {
      case "" => None
      case value => Some(value.toInt)
    }

  private[XMLParse] def parseDate(date: scala.xml.NodeSeq): Long = {
    // DateFormat is not threadsafe, and needs to be recreated for each call
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
    dateFormat.parse(date.text).getTime
  }

  private[XMLParse] def parseOptionDate(date: scala.xml.NodeSeq): Option[Long] = {
    date.text match {
      case "" => None
      case dateText =>
        // DateFormat is not threadsafe, and needs to be recreated for each call
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
        Some(dateFormat.parse(dateText).getTime)
    }
  }


}
