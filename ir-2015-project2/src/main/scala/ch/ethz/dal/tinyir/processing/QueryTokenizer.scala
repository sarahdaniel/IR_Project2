package ch.ethz.dal.tinyir.processing

object QueryTokenizer {
  def tokenize (text: String) : List[String] =
    // blue-white -> blue, white, blue-white
    text.split("[ .,;:?!/\t\n\r\f]+").flatMap(x => x.toLowerCase()+:x.toLowerCase().split("-")).distinct.toList
}