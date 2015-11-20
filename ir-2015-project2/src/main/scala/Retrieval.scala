import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.processing.Tokenizer
import collection.mutable.{ Map => MutMap }
import ch.ethz.dal.tinyir.processing.TipsterCorpusIterator

/**
 * @author ale
 */
object Retrieval {

	val df = MutMap[String, Int]()
	val logtfs = MutMap[String, Map[String, Double]]()

	val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
	val stopWords = io.Source.fromInputStream(stream).mkString.split(",").map(x => x.trim())

	def main(args: Array[String]) {

		//val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
		val zippath = "/Users/sarahdanielabdelmessih/Documents/ETH/Fall2015/InformationRetrieval_workspace/IR1/IR_Project2/ir-2015-project2/src/main/resources/zips/"
		val stream2: java.io.InputStream = getClass.getResourceAsStream("/qrels")
		val bufferedSource2 = io.Source.fromInputStream(stream2)

		//extract relevance judgements for each topic
		val judgements: Map[String, Array[String]] =
			bufferedSource2.getLines()
				.filter(l => !l.endsWith("0"))
				.map(l => l.split(" "))
				.map(e => (e(0), e(2).replaceAll("-", "")))
				.toArray
				.groupBy(_._1)
				.mapValues(_.map(_._2))

		//println(judgements)

		//extract queries
		val topicinputStream = getClass.getResourceAsStream("/topics")
		val doc = new XMLDocument(topicinputStream)

		val words = doc.title.split("Topic:").map(p => p.trim()).filter(p => p != "")
		val cleanwords = words.map(w => Tokenizer.tokenize(stripChars(w, ".,;:-?!%()[]°'\"\t\n\r\f123456789")).filter(!stopWords.contains(_)))

		val numbers = doc.number.split("Number:").map(p => p.trim()).filter(p => p != "").map(p => p.toInt)

		val queries = numbers.zip(cleanwords)

		//set of all the words of the 40 queries
		val querywords = cleanwords.flatten.toSet

		scanDocuments(zippath, querywords)
		//scanDocuments("/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip2",querywords)

		println(df);
		println(logtfs);

		val generalMap = MutMap[Int, Map[String, Double]]()
		val numdocs: Int = df.size

		for (query <- queries) {

			println(query)

			//document frequency of words in query
			val dfquery: Map[String, Double] = (for (w <- query._2) yield (w -> (Math.log10(df.size) - Math.log10(df.getOrElse(w, numdocs).toDouble)))).toMap

			//println(dfquery)

			val queryMap = MutMap[String, Double]()

			for (docprob <- logtfs) {

				//term frequency of words in query
				val tfquery = (query._2).map({ case (k) => (k, docprob._2 getOrElse (k, 0.0)) })

				//println(tfquery)

				//TF-IDF
				val tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
				val score = tfidf.sum
				//println(tfidf)
				//println(score)

				//save only the best 100 documents for each query, otherwise too much memory occupation
				if (queryMap.size == 100) {
					val minscore = queryMap.reduceLeft((l, r) => if (r._2 < l._2) r else l)

					if (score > minscore._2) { //remove min and add this one
						queryMap -= minscore._1
						queryMap += docprob._1 -> score
					}
				} else queryMap += docprob._1 -> score
			}

			//this map contains the best 100 documents for each query in qrels (so 40 x 100 entries)
			// this map must be used to evaluation
			generalMap += query._1 -> queryMap.toMap

			//println(queryMap)

		} //end for query

		//println(generalMap)
	}

	def scanDocuments(folderpath: String, subsetwords: Set[String]) = {

		val tipster = new TipsterCorpusIterator(folderpath) //new TipsterStream(folderpath)
		println("Number of files in zips = " + tipster.length)

		for (doc <- tipster) {

			val tokens = doc.tokens.filter(!stopWords.contains(_)).map(p => p.toLowerCase)

			//document frequency
			df ++= tokens.distinct.filter(w => subsetwords.contains(w)).map(t => t -> (1 + df.getOrElse(t, 0)))

			//log of term frequency
			//logtfs +=  doc.name -> TermFrequencies.logtf(tokens)
			logtfs += doc.name -> logtfSlides(tokens).filter(w => subsetwords.contains(w._1))

		}

	}

	def tf(doc: List[String]): Map[String, Int] =
		doc.groupBy(identity).mapValues(l => l.length)

	def logtfSlides(doc: List[String]): Map[String, Double] =
		logtfSlides(tf(doc))

	def logtfSlides(tf: Map[String, Int]): Map[String, Double] =
		tf.mapValues(v => log2(v.toDouble + 1.0))

	def log2(x: Double) = Math.log10(x) / Math.log10(2.0)

	def stripChars(s: String, ch: String) = s filterNot (ch contains _)

}