import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.processing.Tokenizer
import collection.mutable.{ Map => MutMap }
import ch.ethz.dal.tinyir.processing.TipsterCorpusIterator

object Retrieva{

  val languageModel = false
  val lam = 0.9 //used for the language model

  val df = MutMap[String, Int]()
  val logtfs = MutMap[String, Map[String, Double]]()
  val tfs = MutMap[String, Map[String, Double]]()
  val docLengths = MutMap[String, Double]()
  val collectionFrequencies = MutMap[String, Double]()
  val maxRetrievedDocs = 100

  val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
  val stopWords = io.Source.fromInputStream(stream).mkString.split(",").map(x => x.trim())

  var numDocs = 0
  def main(args: Array[String]) {

    //val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
    //val zippath = "/Users/sarahdanielabdelmessih/Documents/ETH/Fall2015/InformationRetrieval_workspace/IR1/IR_Project2/ir-2015-project2/src/main/resources/zips/"
//    val zippath = "/Users/ale/workspace/inforetrieval/documents/searchengine/zipsAll"
    val zippath = "/home/mim/Documents/Uni/IR_Project2/ir-2015-project2/src/main/resources/zips"

    val qrelsStream: java.io.InputStream = getClass.getResourceAsStream("/qrels")
    val qrelsBufferedSource = io.Source.fromInputStream(qrelsStream)

    //extract relevance judgements for each topic
    val judgements: Map[String, Array[String]] =
      qrelsBufferedSource.getLines()
        .filter(l => !l.endsWith("0"))
        .map(l => l.split(" "))
        .map(e => (e(0), e(2).replaceAll("-", "")))
        .toArray
        .groupBy(_._1)
        .mapValues(_.map(_._2))

    //extract queries
    val topicinputStream = getClass.getResourceAsStream("/topics")
    val doc = new XMLDocument(topicinputStream)

    val words = doc.title.split("Topic:").map(p => p.trim()).filter(p => p != "")
    val cleanwords = words.map(w => Tokenizer.tokenize(stripChars(w, ".,;:-?!%()[]°'\"\t\n\r\f123456789")).filter(!stopWords.contains(_)))

    val numbers = doc.number.split("Number:").map(p => p.trim()).filter(p => p != "").map(p => p.toInt)

    val queries = numbers.zip(cleanwords)

    //set of all the words of the 40 queries
    val querywords = cleanwords.flatten.toSet

    println(querywords)

    scanDocuments(zippath, querywords)

    val generalMap = MutMap[Int, Seq[(String, Double)]]()

    println("Num docs: "+ numDocs);

    for (query <- queries) {
      var fullQueryMap = MutMap[String, Double]()

      if (languageModel) {
        println("fullQueryMap size", fullQueryMap.size)
        fullQueryMap = languageModelScore(query, lam)
        println("fullQueryMap size", fullQueryMap.size)

      }else{
        fullQueryMap = termModelScore(query)
      }

      generalMap += query._1 -> fullQueryMap.toSeq.sortBy(-_._2)

    } //end for query


    //PRINT FIRST 100 DOCUMENTS
    val pw = new java.io.PrintWriter(new java.io.File("result.txt" ))

    val orderedresult = generalMap.toSeq.sortBy(_._1)
    for(query <- orderedresult)
      for (doc <- query._2)
        pw.println(query._1 + " " + doc._1 +" "+doc._2)

    pw.close

    evaluateModel(generalMap, judgements)
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
        println("TP: "+truePos)

        println("Retrieved: "+retrievedDocs.size)
        println("Relevant: "+relevantDocs.size)

        val precisionQuery = truePos.toDouble/retrievedDocs.size
        val recallQuery = truePos.toDouble/relevantDocs.size

        println(query._1 + " Prec: " + precisionQuery + " - Recall: " + recallQuery)

        totalTruePos += truePos
        totalRelevant += relevantDocs.size
      }

       println("Total Prec: " + totalTruePos/totalRetrieved + " - TotalRecall: " + totalTruePos/totalRelevant)
        //gen map   51 -> array[(doc1,10), (doc2,13)]
        //judgenments = 51 -> array(doc1,doc2,doc3)

  }

  def scanDocuments(folderpath: String, subsetwords: Set[String]) = {

    val tipster = new TipsterCorpusIterator(folderpath)

    for (doc <- tipster) {

      val tokens = doc.tokens.filter(!stopWords.contains(_))

      numDocs += 1

      //document frequency - in how many docs is a word present?
      df ++= tokens.distinct.filter(w => subsetwords.contains(w)).map(t => t -> (1 + df.getOrElse(t, 0)))
      docLengths += doc.name -> tokens.length.toDouble

      //keep only query words for the term freq counting
      val queryTokens = tokens.filter(w => subsetwords.contains(w))

      val tfForDoc = tf(queryTokens)
      tfs += doc.name -> tfForDoc
      collectionFrequencies ++= tfForDoc.map{ case (term, freq) => term -> (freq + collectionFrequencies.getOrElse(term, 0.0))}
      logtfs += doc.name -> logtfSlides(tfForDoc)

    }

  }

  /** Calculates the term frequencies for every term in doc */
  def tf(doc: List[String]): Map[String, Double] =
    doc.groupBy(identity).mapValues(l => l.length)

  /** Calculates the logs (x + 1) of every value in the map */
  def logtfSlides(tf: Map[String, Double]): Map[String, Double] =
    tf.mapValues(v => log2(v.toDouble + 1.0))

  def log2(x: Double) = Math.log10(x) / Math.log10(2.0)

  def stripChars(s: String, ch: String) = s filterNot (ch contains _)

  /** Find top 100 ranked docs for a query according to a language probabilistic model.
     * @param query: queryID, query words tuple
     * @param lam: Lambda value for the model
     * @return MuMap with doc -> score
     * */
  def languageModelScore(query: (Int, List[String]) , lam:Double): MutMap[String, Double] = {
     val mapWithScores = MutMap[String, Double]()
     val totalWords = collectionFrequencies.foldLeft(0.0)(_+_._2)

    for ((docName, docTF) <- tfs){
      val docLength = docLengths.getOrElse(docName, 1.0)
      var score = 0.0


      for (word <- query._2) {
        // log P(w|d) = log( (1-lambda)*P`(w|d) + lambda*P(w) )
        val estimatedProb =  docTF.getOrElse(word, 0.0) / docLength
        val priorProb = collectionFrequencies.getOrElse(word, 0.0)/totalWords
        score += log2((1-lam)*estimatedProb + lam*priorProb)
      }

      appendWithMaxSize(mapWithScores, docName, score)

    }
    return mapWithScores
  }

    /** Find top 100 ranked docs for a query according to a TF.IDF model.*/
  def termModelScore(query: (Int, List[String]) ): MutMap[String, Double] = {

      //document frequency of words in query
      val dfquery: Map[String, Double] = (for (w <- query._2) yield (w -> (Math.log10(numDocs) - Math.log10(df.getOrElse(w, numDocs).toDouble)))).toMap

      //println(dfquery)

      val queryMap = MutMap[String, Double]()

      for (docprob <- logtfs) {

        //term frequency of words in query
        val tfquery = (query._2).map({ case (k) => (k, docprob._2 getOrElse (k, 0.0)) })

            //TF-IDF
            val tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
            val score = tfidf.sum

            appendWithMaxSize(queryMap, docprob._1, score)
      }
     return queryMap
  }

  /** This should keep the top 100 ranked documents per query.
   *
   *  If map size is greater than maxRetrievedDocs, remove the smallest score
   *and append the new, bigger score. Otherwise just append.*/
  def appendWithMaxSize(currentMap : MutMap[String, Double], docName:String, score:Double) = {

      if (currentMap.size == maxRetrievedDocs) {
        val minscore = currentMap.reduceLeft((l, r) => if (r._2 < l._2) r else l)

        if (score > minscore._2){//remove min and add this one
          currentMap -= minscore._1
          currentMap += docName -> score
        }
      }else{
        currentMap += docName -> score
      }
  }

}