package ch.ethz.dal.tinyir.processing

import util.Try
import util.Success
import javax.xml.parsers._
import org.w3c.dom.{Document => XMLDoc}
import java.io.InputStream
import ch.ethz.dal.tinyir.io.DocStream


class TipsterParse(is: InputStream) extends XMLDocument(is) { 
  override def title  : String = read(doc.getElementsByTagName("HEAD")) 
  override def body   : String = stripChars(read(doc.getElementsByTagName("TEXT")),".,;:?!%()[]°'\t\n\r\f123456789")
  override def name   : String = read(doc.getElementsByTagName("DOCNO")).filter(_.isLetterOrDigit)
  override def date   : String = ""
  override def content: String = title + " " +body
  def stripChars(s:String, ch:String)= s filterNot (ch contains _)
}

object TipsterParse {
  def main(args: Array[String]) {
    val dirname = "/Users/ale/workspace/inforetrieval/documents/searchengine/zips"
    val fname = dirname + "/DOE2-84-0001"
    val parse = new TipsterParse(DocStream.getStream(fname))
    val name = parse.name
    println(name)    
    val content = parse.content 
    println(content.take(20) + "..." + content.takeRight(20))
  }
}
