import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.processing.QueryTokenizer
//import collection.mutable.{ Map => MutMap }

import com.github.aztek.porterstemmer.PorterStemmer
import scala.collection.mutable.{ OpenHashMap => MutMap}
import ch.ethz.dal.tinyir.processing.TipsterCorpusIterator
import java.io.File

//import net.didion.jwnl.JWNL

object Retrieval{

  val languageModel = true
  val lam = 0.3 //used for the language model
  val mu= 2000
  val fullSet = true
  val maxRetrievedDocs = 10


  val df = MutMap[String, Int]()
  //todelete -> val logtfs = MutMap[String, Map[String, Double]]()
  val tfs = MutMap[String, Map[String, Double]]()
  val docLengths = MutMap[String, Double]()
  val collectionFrequencies = MutMap[String, Double]()
  var allEvaluatedDocs = Set[String]()
  var parsedJudgements = Map[String, Array[String]]()


  val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
  val stopWords = io.Source.fromInputStream(stream).mkString.split(",").map(x => x.trim())

  var numDocs = 0
  def main(args: Array[String]) {

    //val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
//    val zippath = "/Users/sarahdanielabdelmessih/git/IR_Project2/ir-2015-project2/src/main/resources/testZips"
    //val zippath = "/Users/ale/workspace/inforetrieval/documents/searchengine/zipsAll"
    val zippath = "/home/mim/Documents/Uni/IR_Project2/ir-2015-project2/src/main/resources/zips"

    val judgements = parseRelevantJudgements("/qrels")

    //needed to score only on evaluated documents
    parseJudgementsForEvaluation("/qrels")

    //val propertiesURL = getClass.getResource("/file_properties.xml"
//    val wn = new Wordnet(new File(propertiesURL.getPath()))

    //extract queries
    val (queries, querywords) = extractQueries("/topics")

    //println(queries);
   for (query <- queries){
     println(query._2)

   }

    //println(querywords)
    println("Scanning documents at path " + zippath)
    scanDocuments(zippath, querywords)

    val generalMap = MutMap[Int, Seq[(String, Double)]]()

    println("Number of documents in collection: "+ numDocs);
    println("Number of queries: ", queries.length)

    for (query <- queries) {
      println(query._1)
      var fullQueryMap = MutMap[String, Double]()

      if (languageModel) {
//        topDocs = rankWithLanguageModel(query, lam, 10)

        fullQueryMap = rankWithLanguageModel(query, lam, maxRetrievedDocs)

      }else{
        fullQueryMap = rankWithTermModel(query, maxRetrievedDocs)
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

    evaluateModel(generalMap, judgements, queries.toMap)
    //println(generalMap)
  }


   /** Parses all relevant judgements
    *  @return Map query-> list of relevant doc ids
    *  */
  def parseRelevantJudgements(relevanceJudgementsPath:String): Map[String, Array[String]] = {
    val qrelsStream: java.io.InputStream = getClass.getResourceAsStream(relevanceJudgementsPath)
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

    allEvaluatedDocs = judgements.values.flatten.toSet
    return judgements
  }


  /** Parses all relevance judgement, collecting all judged relevant and
   *irrelevant docs*/
  def parseJudgementsForEvaluation(relevanceJudgementsPath:String){

    //reset is broken in scala
    val qrelsStream2: java.io.InputStream = getClass.getResourceAsStream(relevanceJudgementsPath)
    val qrelsBufferedSource2 = io.Source.fromInputStream(qrelsStream2)

    parsedJudgements = qrelsBufferedSource2.getLines()
        .map(l => l.split(" "))
        .map(e => (e(0), e(2).replaceAll("-", "")))
        .toArray
        .groupBy(_._1)
        .mapValues(_.map(_._2))

    }

  /** Parses the topics file, extacting all topic IDs and titles, forming a
   *set of query words from all topics/queries. */
  def extractQueries(topicsPath:String): (Array[(Int, List[String])], Set[String]) = {
    //extract queries
    val topicinputStream = getClass.getResourceAsStream(topicsPath)
    val doc = new XMLDocument(topicinputStream)

    val words = doc.title.split("Topic:").map(p => p.trim()).filter(p => p != "")
    //-----> PORTER STEMMER
//    val cleanwords = words.map(w => QueryTokenizer.tokenize(stripChars(w, "123456789)(\"")).filter(!stopWords.contains(_)).map(PorterStemmer.stem(_)))
    val cleanwords = words.map(w => QueryTokenizer.tokenize(stripChars(w, "123456789)(\"")).filter(!stopWords.contains(_)))
    val numbers = doc.number.split("Number:").map(p => p.trim()).filter(p => p != "").map(p => p.toInt)
    val queries = numbers.zip(cleanwords)

   //set of all the words of the 40 queries
    val querywords = cleanwords.flatten.toSet

    return (queries, querywords)
  }


  def evaluateModel(generalMap :MutMap[Int, Seq[(String, Double)]], judgements:Map[String, Array[String]], queries:Map[Int, List[String]]){

     var totalTruePos : Double=0
     val totalRetrieved = maxRetrievedDocs * judgements.size //100 docs for each query
     var totalRelevant: Double=0
     var totalF1: Double =  0

      for(query <- generalMap){

        val retrievedDocs= (query._2).map({case(x,y) => x})
        val relevantDocs= judgements.get(query._1.toString) match {case Some(doc) => doc}

        val truePos = (retrievedDocs intersect relevantDocs).size

        println("Query: " + query._1 + " " + queries.get(query._1))
        println("TP: "+truePos)

        println("Retrieved: "+retrievedDocs.size)
        println("Relevant: "+relevantDocs.size)

        val precisionQuery = truePos.toDouble/retrievedDocs.size
        val recallQuery = truePos.toDouble/relevantDocs.size
        val f1 = (2.0 * precisionQuery *  recallQuery) / (precisionQuery *  recallQuery)

        val ap = 0.0//calculateAveragePrecision()

        println("Prec: " + precisionQuery + " - Recall: " + recallQuery)

        println("------------------------------------------------")

        totalTruePos += truePos
        totalRelevant += relevantDocs.size
        totalF1 += f1
      }

       println("Total Prec: " + totalTruePos/totalRetrieved + " - TotalRecall: " + totalTruePos/totalRelevant + "Total F1: " + totalF1/(generalMap.size))
        //gen map   51 -> array[(doc1,10), (doc2,13)]
        //judgenments = 51 -> array(doc1,doc2,doc3)

  }

  def calculateAveragePrecision(numRetrievedDocs: Int): Double =
  {
     var avgPrecision : Double = 0.0

        for( k <- 1 to numRetrievedDocs)
        {
            //precission at rank k:
        }
     return avgPrecision

  }

  def scanDocuments(folderpath: String, subsetwords: Set[String]) = {

    val tipster = new TipsterCorpusIterator(folderpath)

    for (doc <- tipster) {

      // ---> PORTER STEMMER
//      val tokens = doc.tokens.map(PorterStemmer.stem(_)).filter(!stopWords.contains(_))
      val tokens = doc.tokens.filter(!stopWords.contains(_))

      numDocs += 1

      docLengths += doc.name -> tokens.length.toDouble

      //keep only query words for the term freq counting
      val queryTokens = tokens.filter(w => subsetwords.contains(w))

      //document frequency - in how many docs is a query word present?
      df ++= queryTokens.distinct.map(t => t -> (1 + df.getOrElse(t, 0)))

      val tfForDoc = tf(queryTokens)
      tfs += doc.name -> tfForDoc

      //smooth with 0.1 so that we don't get -infinity for VERY rare words
      collectionFrequencies ++= tfForDoc.map{ case (term, freq) => term -> (freq + collectionFrequencies.getOrElse(term, 0.1))}

      if (numDocs%1000 ==0){
        println("Processed document: " + numDocs)
     }

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


  /** Find top <numDocsToRetrieve> ranked docs for a query according to a language probabilistic model.
     * @param query: queryID, query words tuple
     * @param lam: Lambda value for the model
     * @param numDocsToRetrieve: number of documents to retrieve ordered by score
     * @return MuMap with doc -> score
     * */
  def rankWithLanguageModel(query: (Int, List[String]) , lam:Double, numDocsToRetrieve:Int): MutMap[String, Double] = {
     val relDocs = parsedJudgements.getOrElse(query._1.toString(), Array[String]()).toSet


     val mapWithScores = MutMap[String, Double]()
     val totalWords = collectionFrequencies.foldLeft(0.0)(_+_._2)

    for ((docName, docTF) <- tfs){
      if (fullSet || relDocs.contains(docName)){
        val docLength = docLengths.getOrElse(docName, 1.0)
            var score = 0.0

            //JEKELIN-MERCER
//            for (word <- query._2) {
//              // log P(w|d) = log( (1-lambda)*P`(w|d) + lambda*P(w) )
//              val estimatedProb =  docTF.getOrElse(word, 0.0) / docLength
//                  val priorProb = collectionFrequencies.getOrElse(word, 0.1)/totalWords
//                  score += log2((1-lam)*estimatedProb + lam*priorProb)
//            }

//            //DIRICHLET SMOOTHING
//            //log P(w|d) =  log( (tf + µ * P(w)/(|d| + µ))
//            for (word <- query._2) {
//              val tf =  docTF.getOrElse(word, 0.0)
//                  val priorProb = collectionFrequencies.getOrElse(word, 0.1)/totalWords
//                  score += log2((tf + mu * priorProb) / (docLength + mu))
//            }
//
            //TWO-STAGE SMOOTHING --> http://sifaka.cs.uiuc.edu/czhai/pub/sigir2002-twostage.pdf
            //log P(w|d) =  log((1-lambda) * ((tf + µ * P(w)/(|d| + µ)) + lambda * P(w))
            for (word <- query._2) {
              val tf =  docTF.getOrElse(word, 0.0)
                  val priorProb = collectionFrequencies.getOrElse(word, 0.1)/totalWords
                  score += log2((1-lam)*((tf + mu * priorProb) / (docLength + mu)) + (lam * priorProb)  )
            }

        appendWithMaxSize(mapWithScores, docName, score, numDocsToRetrieve)

      }
    }
    return mapWithScores
  }


    /** Find top 100 ranked docs for a query according to a TF.IDF model.*/
  def rankWithTermModel(query: (Int, List[String]), numDocsToRetrieve:Int ): MutMap[String, Double] = {

      val relDocs = parsedJudgements.getOrElse(query._1.toString(), Array[String]()).toSet

      //document frequency of words in query
      val dfquery: Map[String, Double] = (for (w <- query._2) yield (w -> (Math.log10(numDocs) - Math.log10(df.getOrElse(w, numDocs).toDouble)))).toMap

      //println(dfquery)

      val queryMap = MutMap[String, Double]()

      for (docprob <- tfs) {
        if (fullSet || relDocs.contains(docprob._1)) {

          //log term frequency
          val logtf = logtfSlides(docprob._2)
          //term frequency of words in query
          val tfquery = (query._2).map({ case (k) => (k, logtf getOrElse (k, 0.0)) })

              //TF-IDF
              val tfidf = dfquery.map({ case (k, v) => tfquery map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten
              val score = tfidf.sum

              appendWithMaxSize(queryMap, docprob._1, score, numDocsToRetrieve)
        }
      }

     return queryMap
  }

  /** This should keep the top 100 ranked documents per query.
   *
   *  If map size is greater than maxRetrievedDocs, remove the smallest score
   *and append the new, bigger score. Otherwise just append.*/
  def appendWithMaxSize(currentMap : MutMap[String, Double], docName:String, score:Double, numDocsToRetrieve:Int) = {

      if (currentMap.size == numDocsToRetrieve) {
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