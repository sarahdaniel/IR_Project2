import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.processing.Tokenizer
import collection.mutable.{ Map => MutMap }
import ch.ethz.dal.tinyir.processing.TipsterCorpusIterator

/**
 * @author ale
 */
object RetrievalOrig {

	val df = MutMap[String, Int]()
	val logtfs = MutMap[String, Map[String, Double]]()
  val tfs = MutMap[String, Map[String, Int]]()
  val docLengths = MutMap[String, Int]()
  val collectionFrequencies = MutMap[String, Double]()
  
  var totalLength =0.0
  
  //FOR AUGMENTED TF
  val maxFreq = MutMap[String, Int]()
  
  val maxRetrievedDocs = 100

	val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
	val stopWords = io.Source.fromInputStream(stream).mkString.split(",").map(x => x.trim())
  
  val normaltdidf=false;
  val augmentedts= false;
  val okapibm25 = true;

	var numDocs = 0
	def main(args: Array[String]) {

		//val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
		//val zippath = "/Users/sarahdanielabdelmessih/Documents/ETH/Fall2015/InformationRetrieval_workspace/IR1/IR_Project2/ir-2015-project2/src/main/resources/zips/"
		val zippath = "/Users/ale/workspace/inforetrieval/documents/searchengine/zipsAll"
    
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

       /* for(judg <- judgements)
          for (doc <- judg._2)
            println(judg._1 + " " +doc)*/
            
		//println(judgements)

		//extract queries
		val topicinputStream = getClass.getResourceAsStream("/topics")
		val doc = new XMLDocument(topicinputStream)

		val words = doc.title.split("Topic:").map(p => p.trim()).filter(p => p != "")
		val cleanwords = words.map(w => Tokenizer.tokenize(stripChars(w, "123456789)(\"")).filter(!stopWords.contains(_)))

		val numbers = doc.number.split("Number:").map(p => p.trim()).filter(p => p != "").map(p => p.toInt)

		val queries = numbers.zip(cleanwords)

		//set of all the words of the 40 queries
		val querywords = cleanwords.flatten.toSet

		scanDocuments(zippath, querywords)

		//--println(df);
    //--println("df size" + df.size);

    val generalMap = MutMap[Int, Seq[(String, Double)]]()
		val numdocs: Int = numDocs //df.size

    println("Num docs: "+numdocs);
    
    val avgdl = totalLength/numdocs
    
		for (query <- queries) {

			println(query)

			//document frequency of words in query
			val dfquery: Map[String, Double] = (for (w <- query._2) yield (w -> (Math.log10(numdocs) - Math.log10(df.getOrElse(w, numdocs).toDouble)))).toMap

			//println(dfquery)

			val queryMap = MutMap[String, Double]()

			for (docprob <- tfs) {

        var tfidf:Iterable[Double]= Nil
        
        if(normaltdidf){
        //1) normal tf
          val logtf = logtfSlides(docprob._2)
  				//term frequency of words in query
  				val tfquery = (query._2).map({ case (k) => (k, logtf getOrElse (k, 0.0)) })
          //TF-IDF (normal or augmented)
           tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
 
        }else if(augmentedts) {
        //2) augmented tf
          val augtf = (docprob._2).mapValues(v => 0.5 + (0.5 * v.toDouble)/(maxFreq.getOrElse(docprob._1,1).toDouble))
          //term frequency of words in query
          val tfquery = (query._2).map({ case (k) => (k, augtf getOrElse (k, 0.0)) })
          //TF-IDF (normal or augmented)
           tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
 
        }else if(okapibm25){       
        //3) OKAPI BM25
          val b= 0.75
          val k= 1.5
          val dl= docLengths.getOrElse(docprob._1,0)   
          val okapitf= (docprob._2).mapValues(v => ((k+1)*v) / (k * ((1-b) +b* dl/avgdl) +v))
        
          val okapidf = dfquery.mapValues(v=> Math.log((numdocs - v +0.5)/(v+0.5)))
          
          //OKAPI BM25
          tfidf = okapidf.map({ case (k, v) => okapitf map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
        }
				//println(tfquery)

				//TF-IDF (normal or augmented)
				//val tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
            
        
				val score = tfidf.sum
				//println(tfidf)
				//println(score)

				//save only the best 100 documents for each query, otherwise too much memory occupation
				if (queryMap.size == maxRetrievedDocs) {
					val minscore = queryMap.reduceLeft((l, r) => if (r._2 < l._2) r else l)

					if (score > minscore._2) { //remove min and add this one
						queryMap -= minscore._1
						queryMap += docprob._1 -> score
					}
				} else queryMap += docprob._1 -> score
			}

			//this map contains the best 100 documents for each query in qrels (so 40 x 100 entries)
			// this map must be used to evaluation
      generalMap += query._1 -> queryMap.toSeq.sortBy(-_._2)

		} //end for query
    
    
    //PRINT FIRST 100 DOCUMENTS
    val pw = new java.io.PrintWriter(new java.io.File("result.txt" ))

    val orderedresult = generalMap.toSeq.sortBy(_._1)
    for(query <- orderedresult)
      for (doc <- query._2)
        pw.println(query._1 + " " + doc._1 +" "+doc._2)
        
    pw.close
    
    evaluateModel(generalMap,judgements)
		//println(generalMap)
	}
  
   
  def evaluateModel(generalMap :MutMap[Int, Seq[(String, Double)]], judgements:Map[String, Array[String]]){
    
     var totalTruePos : Double=0
     val totalRetrieved = maxRetrievedDocs * judgements.size //100 docs for each query
     var totalRelevant: Double=0
     
      for(query <- generalMap){
                                                          
        val retrievedDocs= (query._2).map({case(x,y) => x}) 
        val relevantDocs= judgements.get(query._1.toString) match {case Some(doc) => doc}
  
        val truePos = (retrievedDocs intersect relevantDocs).size
        
        println("Query: " + query._1)
        println("TP: "+truePos)
        
        println("Retrieved: "+retrievedDocs.size)
        println("Relevant: "+relevantDocs.size)
        
        val precisionQuery = truePos.toDouble/retrievedDocs.size
        val recallQuery = truePos.toDouble/relevantDocs.size
        
        println("Prec: " + precisionQuery + " - Recall: " + recallQuery)        
        println("------------------------------------------------")
        
        totalTruePos += truePos
        totalRelevant += relevantDocs.size
      }
      
       println("Total Prec: " + totalTruePos/totalRetrieved + " - TotalRecall: " + totalTruePos/totalRelevant)
        //gen map   51 -> array[(doc1,10), (doc2,13)]
        //judgenments = 51 -> array(doc1,doc2,doc3)
        
  }

	def scanDocuments(folderpath: String, subsetwords: Set[String]) = {

		val tipster = new TipsterCorpusIterator(folderpath) //new TipsterStream(folderpath)
		//println("Number of files in zips = " + tipster.length)

		for (doc <- tipster) {

			val tokens = doc.tokens.filter(!stopWords.contains(_))
      
			numDocs += 1

      println(numDocs)
      
      val queryTokens = tokens.filter(w => subsetwords.contains(w))
       
      //docLenght
      docLengths += doc.name -> tokens.length
      
      totalLength += tokens.length
       
			//document frequency
			df ++= queryTokens.distinct.map(t => t -> (1 + df.getOrElse(t, 0)))

      /*
      //FOR AUGMENTED TF      
      val tokval = tf(tokens).values     
      if(tokval.size>0)
        maxFreq += doc.name -> (tf(tokens).values.max.toInt)
      */
      
      val tfForDoc = tf(queryTokens)
      tfs += doc.name -> tfForDoc
 
      collectionFrequencies ++= tfForDoc.map{ case (term, freq) => term -> (freq + collectionFrequencies.getOrElse(term, 0.0))}

		}

	}


    
	def tf(doc: List[String]): Map[String, Int] =
		doc.groupBy(identity).mapValues(l => l.length)

	//def logtfSlides(doc: List[String]): Map[String, Double] =
	//	logtfSlides(tf(doc))

	def logtfSlides(tf: Map[String, Int]): Map[String, Double] =
		tf.mapValues(v => log2(v.toDouble + 1.0))

	def log2(x: Double) = Math.log10(x) / Math.log10(2.0)

	def stripChars(s: String, ch: String) = s filterNot (ch contains _)

}