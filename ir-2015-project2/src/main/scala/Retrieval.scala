import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.lectures.TermFrequencies
import collection.mutable.{Map => MutMap}

/**
 * @author ale
 */
object Retrieval {
  
def main (args:Array[String]){
     
  val zippath = "/Users/ale/workspace/inforetrieval/Documents/searchengine/testzip"
  
  val stream: java.io.InputStream = getClass.getResourceAsStream("/stopwords.txt")
  val stopWords =io.Source.fromInputStream(stream).mkString.split(",").map(x=> x.trim())
  
  val stream2: java.io.InputStream = getClass.getResourceAsStream("/qrels")
  val bufferedSource2 = io.Source.fromInputStream(stream2)
     
 
  //extract relevance judgements for each topic
  val judgements: Map[String, Array[String]] =
  bufferedSource2.getLines()
  .filter(l => !l.endsWith("0"))
  .map(l => l.split(" "))
  .map(e => (e(0),e(2).replaceAll("-", "")))
  .toArray
  .groupBy(_._1)
  .mapValues(_.map(_._2))      
  
  println(judgements)
  
  
  // !!!! MODIFICATA TIPSTERPARSE.SCALA in tiniyIR x includere il TITOLO !!!!

  val tipster = new TipsterStream(zippath)
  //val tipster = new TipsterStream ("/Users/ale/workspace/inforetrieval/Documents/searchengine/zips")
                                                
   println("Number of files in zips = " + tipster.length)
                                                 
   val df = MutMap[String,Int]()                  
   val logtfs = MutMap[String,Map[String,Double]]()
                                                      
   for (doc <- tipster.stream) {
   
     val tokens = doc.tokens.filter(!stopWords.contains(_)).map(p=>p.toLowerCase)
     //println(tokens)
     
     
     df ++= tokens.distinct.map(t => t-> (1+df.getOrElse(t,0)))
     
     //logtfs +=  doc.name -> TermFrequencies.logtf(tokens)
     logtfs +=  doc.name -> logtfSlides(tokens)
     
   }
  
  println(df);
  println(logtfs);
  
  val generalMap = MutMap[Int,Map[String,Double]]()    
  
  
  //-----> for (query <- querylist)  TO DO !!!!! READ QUERIES FROM TOPICS FILE
  val query =List("support","lead","third","pippopluto")  
 
  val numdocs : Int = df.size
  
  //val d : Map[String,Double] =query.map({case(k) => (k, (Math.log10(df.size) - Math.log10(df.getOrElse(k,c))).toDouble)}).toMap
  val dfquery : Map[String,Double] =(for (w <- query) yield (w -> (Math.log10(df.size) - Math.log10(df.getOrElse(w,numdocs).toDouble)))).toMap
  
  println(dfquery)
 
  val queryMap = MutMap[String,Double]()    
  
  for (docprob <- logtfs){
      
      val tfquery=query.map({case(k) => (k, docprob._2 getOrElse(k,0.0))})

      println(tfquery)
      val tfidf = dfquery.map({case(k,v) => tfquery map({case(x,y) => if(k==x) v*y else 0.0})}).flatten
      val score = tfidf.sum
      println(tfidf)
      println(score)
      
      //save only the best 100 documents for each query, otherwise too much memory occupation
      if (queryMap.size == 100){ 
        val minscore = queryMap.reduceLeft((l,r) => if (r._2 < l._2) r else l)
  
        if(score > minscore._2){ //remove min and add this one
          queryMap-= minscore._1
          queryMap+= docprob._1 -> score
        }
      } else  queryMap+= docprob._1 -> score
  }
  
  //this map contains the best 100 documents for each query in qrels (so 40 x 100 entries)
  // this map must be used to evaluation
    generalMap += 51 -> queryMap.toMap 
    
    println(queryMap)
  
 //-----> } //end for query
    
    println(generalMap)
}   

def tf(doc : List[String]) : Map[String, Int] = 
    doc.groupBy(identity).mapValues(l => l.length)
  

def logtfSlides(doc : List[String]) : Map[String, Double] = 
    logtfSlides(tf(doc))

    
def logtfSlides(tf: Map[String,Int]) : Map[String, Double] = 
    tf.mapValues(v => log2(v.toDouble +1.0))

def log2 (x : Double) = Math.log10(x)/Math.log10(2.0)
   
  
}