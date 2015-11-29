import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.processing.QueryTokenizer
//import collection.mutable.{ Map => MutMap }

import com.github.aztek.porterstemmer.PorterStemmer
import scala.collection.mutable.{ OpenHashMap => MutMap}
import ch.ethz.dal.tinyir.processing.TipsterCorpusIterator
import java.io.File


object Retrieval{

  val languageModel = false
  
  //term based model parameters
  val b= 0.35
  val k= 1
    
  //language model parameters
  val lam = 0.1 
  val mu= 1000

  val maxRetrievedDocs = 100

  val df = MutMap[String, Int]()
  val tfs = MutMap[String, Map[String, Double]]()
  val docLengths = MutMap[String, Double]()
  val collectionFrequencies = MutMap[String, Double]() 
  var totalLength =0.0


  val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
  val stopWords = io.Source.fromInputStream(stream).mkString.split(",").map(x => x.trim())

  var numDocs = 0
  def main(args: Array[String]) {

    //val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
//    val zippath = "/Users/sarahdanielabdelmessih/git/IR_Project2/ir-2015-project2/src/main/resources/zips"
    val zippath = "/Users/ale/IR/zipsAll"
    //val zippath = "/home/mim/Documents/Uni/IR_Project2/ir-2015-project2/src/main/resources/zips"

    val judgements = parseRelevantJudgements("/qrels")

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
    val avgdl = totalLength/numDocs

    println("Number of documents in collection: "+ numDocs);
    println("Number of queries: "+ queries.length)

    
    for (query <- queries) {
      println(query._1)
      var fullQueryMap = MutMap[String, Double]()

      if (languageModel) {

        fullQueryMap = rankWithLanguageModel(query)

      }else{
        fullQueryMap = rankWithTermModel(query,avgdl)
      }

      generalMap += query._1 -> fullQueryMap.toSeq.sortBy(-_._2)

    } //end for query


    //PRINT FIRST 100 DOCUMENTS
    val pw = new java.io.PrintWriter(new java.io.File("result.txt" ))

    val orderedresult = generalMap.toSeq.sortBy(_._1)
    for(query <- orderedresult){
      val docwithIndex = (query._2).map({case (x,y) => x}).zipWithIndex
      for (doc <- docwithIndex)
        pw.println(query._1 + " " + (doc._2 +1) +" "+doc._1)
    }

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

    return judgements
  }


  /** Parses the topics file, extracting all topic IDs and titles, forming a
   *set of query words from all topics/queries. */
  def extractQueries(topicsPath:String): (Array[(Int, List[String])], Set[String]) = {
    //extract queries
    val topicinputStream = getClass.getResourceAsStream(topicsPath)
    val doc = new XMLDocument(topicinputStream)

    val words = doc.title.split("Topic:").map(p => p.trim()).filter(p => p != "")
    //-----> PORTER STEMMER
    //val cleanwords = words.map(w => QueryTokenizer.tokenize(stripChars(w, "123456789)(\"")).filter(!stopWords.contains(_)).map(PorterStemmer.stem(_)))
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
     var totalF1: Double =  0.0
     var meanAveragePrecision: Double = 0.0

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

        var f1 = 0.0
        if(precisionQuery!= 0 && recallQuery != 0)
        {
          f1 = (2.0 * precisionQuery *  recallQuery) / (precisionQuery +  recallQuery)
          }

        val ap = calculateAveragePrecision(query._2, relevantDocs)

        println("Prec: " + precisionQuery + " - Recall: " + recallQuery + " - F1: " + f1 + " Average Precision: " + ap)

        println("------------------------------------------------")

        totalTruePos += truePos
        totalRelevant += relevantDocs.size
        totalF1 += f1
        
        meanAveragePrecision += ap
      }

       println("Total Prec: " + totalTruePos/totalRetrieved + " - TotalRecall: " + totalTruePos/totalRelevant + " Total F1: " + totalF1/(generalMap.size) + " Mean Average Precision: " + meanAveragePrecision/generalMap.size)

  }

  def calculatePrecision(docRanks: Seq[(String, Double)], relevantDocs:Seq[String]): Double=
  {
      
      val retrievedDocs= docRanks.map({case(x,y) => x})
       
      val truePos = (retrievedDocs intersect relevantDocs).size
      val precision = truePos.toDouble/retrievedDocs.size
       

    return precision
  }
  def calculateAveragePrecision(docRanks: Seq[(String, Double)], relevantDocs:Seq[String]): Double =
  {
     var avgPrecision : Double = 0.0

        for( ((docName, score),k) <- docRanks.zipWithIndex)
        {
          if(relevantDocs.contains(docName)){
          //precision at rank k: 
           avgPrecision += calculatePrecision(docRanks.take(k + 1), relevantDocs)
          }
        }
     
     return avgPrecision/relevantDocs.size

  }

  def scanDocuments(folderpath: String, subsetwords: Set[String]) = {

    val tipster = new TipsterCorpusIterator(folderpath)

    for (doc <- tipster) {

      // ---> PORTER STEMMER
      //val tokens =   doc.tokens.filter(!stopWords.contains(_)).map(PorterStemmer.stem(_))
      val tokens = doc.tokens.filter(!stopWords.contains(_))

      numDocs += 1

      docLengths += doc.name -> tokens.length.toDouble
      
      totalLength += tokens.length

      //keep only query words for the term freq counting
      val queryTokens = tokens.filter(w => subsetwords.contains(w))

      //document frequency - in how many docs is a query word present?
      df ++= queryTokens.distinct.map(t => t -> (1 + df.getOrElse(t, 0)))

      val tfForDoc = tf(queryTokens)
      tfs += doc.name -> tfForDoc

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


  /** Find top <maxRetrievedDocs> ranked docs for a query according to a language probabilistic model.
     * @param query: queryID, query words tuple
     * @return MuMap with doc -> score
     * */
  def rankWithLanguageModel(query: (Int, List[String])): MutMap[String, Double] = {


     val mapWithScores = MutMap[String, Double]()
     val totalWords = collectionFrequencies.foldLeft(0.0)(_+_._2)

    for ((docName, docTF) <- tfs){
        val docLength = docLengths.getOrElse(docName, 1.0)
            var score = 0.0

            //TWO-STAGE SMOOTHING --> http://sifaka.cs.uiuc.edu/czhai/pub/sigir2002-twostage.pdf
            //log P(w|d) =  log((1-lambda) * ((tf + µ * P(w)/(|d| + µ)) + lambda * P(w))
            for (word <- query._2) {
              val tf =  docTF.getOrElse(word, 0.0)
                  val priorProb = collectionFrequencies.getOrElse(word, 0.1)/totalWords
                  score += log2((1-lam)*((tf + mu * priorProb) / (docLength + mu)) + (lam * priorProb)  )
            }

        appendWithMaxSize(mapWithScores, docName, score)
    }
    return mapWithScores
  }


    /** Find top 100 ranked docs for a query according to a TF.IDF model.*/
  def rankWithTermModel(query: (Int, List[String]), avgdl: Double): MutMap[String, Double] = {

      //document frequency of words in query
      val dfquery: Map[String, Double] = (for (w <- query._2) yield (w -> (Math.log10(numDocs) - Math.log10(df.getOrElse(w, numDocs).toDouble)))).toMap

      //println(dfquery)

      val queryMap = MutMap[String, Double]()

      for (docprob <- tfs) {

          //OKAPI BM25
          val dl= docLengths.getOrElse(docprob._1,0.0)   
          val okapitf= (docprob._2).mapValues(v => ((k+1)*v) / (k * ((1-b) +b* dl/avgdl) +v))
        
          val okapidf = dfquery.mapValues(v=> Math.log((numDocs - v +0.5)/(v+0.5)))
          
          val tfidf = okapidf.map({ case (k, v) => okapitf map ({ case (x, y) => if (k == x) v * y else 0.0 }) }).flatten

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