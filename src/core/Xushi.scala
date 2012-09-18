package core
import scala.collection.immutable.HashMap
import com.esotericsoftware.kryonet.Listener
import com.esotericsoftware.kryonet.Connection
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.collection.immutable.TreeMap
import scala.collection.immutable.Set
import com.esotericsoftware.kryonet.Server
import com.esotericsoftware.kryonet.KryoSerialization
import scala.collection.immutable.TreeSet

class Xushi {
  var store = new TreeMap[String, Option[Any]]
  //var peers = new HashMap[StoreConnection, Map[String, List[String]]]
  var peers2 = List[StoreConnection]()

  def diceKey(key:String)=
    key.split("\\.").inits.filterNot(_.isEmpty).map((x) => x reduce (_.concat(".") ++ _)).toStream.reverse
  
  def interested(key: String, getList:(StoreConnection)=>List[String]) = 
    peers2.filter((p)=>diceKey(key).exists(getList(p).contains(_)))
    
  def add(key: String, v: Any) = {
    diceKey(key).foreach((mk)=> store.get(mk).getOrElse{store = store + ((mk, None))})
    store = store + ((key, Some(v)))
    interested(key, _.changeListeners) foreach (sendAdd(_)(key, v))
  }

  val sendAdd = (to: StoreConnection)=>(x:(String, Any))=> x match {
    case (key, v) => {
    	println("sending to " + to.getID() + " " + key + " " + v)
    	to sendTCP (new ADD(key, v))
    }
  } 
    
  val itemsForUpdate = (key:String)=>store.from(key).takeWhile(_._1.startsWith(key)).filter(_._2.isDefined).map((x)=>(x._1, x._2.get))

  val server = new StoreServer(9999)

  def init = {
    server.accept()
    server.server.addListener(new Listener {
      override def connected(c: Connection) {
        val conn = c.asInstanceOf[StoreConnection]
        peers2 = conn :: peers2
      }
      override def disconnected(c: Connection) {
        val conn = c.asInstanceOf[StoreConnection]
        peers2 = peers2 - conn
      }
      
      def removeFirst[A](item: A, list: List[A]) = list diff List(item)
      
      override def received(c: Connection, obj: Object) {
        val conn = c.asInstanceOf[StoreConnection]
        obj match {
          case c: CMD => c match {
            case ADD(key, v) => add(key, v)
            //case DEL(key)    => del(key)
            case LISTEN(key,what) => {
              println ("got listen " + key)
              what match {
                case x if x == "ADD" => 
	              conn.changeListeners = key :: conn.changeListeners  
	              itemsForUpdate(key).foreach(sendAdd(conn))
                case x if x == "DEL" =>
	              conn.delListeners = key :: conn.delListeners 
              }
            }
            case UNLISTEN(key,what)=>{
              println ("got unlisten " + key)
              what match {
                case x if x == "ADD" => conn.changeListeners =removeFirst(key,conn.changeListeners)
                case x if x == "DEL" => conn.delListeners = removeFirst(key,conn.changeListeners)
              }
            }
            
          }
          case _=>
        }
      }
    })
  }
}

class StoreConnection extends Connection {
  var changeListeners = List[String]()
  var delListeners = List[String]()
}

class StoreServer(port: Int) extends Store(port) {
  val server = new Server(16384, 2048, new KryoSerialization(kryi)) {
    override def newConnection(): Connection = {
      new StoreConnection
    }
  }

  val accept = () => {
    server.bind(port)
    server.addListener(new Listener {
      override def connected(c: Connection) {
        println("new connection")
      }
      override def received(c: Connection, obj: Object) {
        val conn = c.asInstanceOf[StoreConnection]
      }
    })

    server.start()
  }
}


object Xushi {
  def main(args: Array[String]) {
    val a = new Xushi;
    a.init
  }
}