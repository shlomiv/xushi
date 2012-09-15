package core
import scala.collection.immutable.HashMap

class Xushi {
	var store = new HashMap[String, Any]
	var peers = new HashMap[String, List[String]]
	
	def interested(key:String) = {
	 val diced = key.split("\\.").inits.filterNot(_.isEmpty).map((x)=>x reduce (_.concat(".") ++ _)).toStream.reverse
	 peers.filter((p)=>diced.exists(p._2.contains(_))).map(_._1)
	}
	     		
	def add(key:String, v:Any) = {
	 store + ((key, v))
	 interested(key) foreach (send(key, v)(_))
	}
	
	val send=(key:String, v:Any)=>(to:String)=>println("sending to " + to +" " + key + " " + v)
	
	//def onChange(key:String, e:(String, Any)=>Unit) = null
	
}

object Xushi {
  def main(args: Array[String]) {
	  val a = new Xushi;
	   a.peers = a.peers + ("a" -> List("key1", "key2")) + ("b" -> List("key1.key6", "key2.key5"))
	   a.add("key1.key6.k1.2.34.5.6.778.fsfg.sd.er", 23)
  }
}