package core
import scala.collection.immutable.SortedMap
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryonet.Client
import com.esotericsoftware.kryonet.Connection
import com.esotericsoftware.kryonet.KryoSerialization
import com.esotericsoftware.kryonet.Listener
import com.esotericsoftware.kryonet.Server
import com.esotericsoftware.kryo._
import scala.collection.immutable.TreeMap

abstract class Store[A](val port: Int) {
  val kryi = new Kryo

  object CmdSerialization {
    def register(k: Kryo) {
      k.register(classOf[ADD], new ADDSerializer)
      k.register(classOf[LISTEN], new LISTENSerializer)
      k.register(classOf[DEL], new DELSerializer)
    }

    class LISTENSerializer extends Serializer[LISTEN] {
      override def read(kryo: Kryo, input: Input, t: Class[LISTEN]): LISTEN = LISTEN(input.readString())
      override def write(kryo: Kryo, output: Output, v: LISTEN) = output.writeString(v.key)
    }
    class DELSerializer extends Serializer[DEL] {
      override def read(kryo: Kryo, input: Input, t: Class[DEL]): DEL = DEL(input.readString())
      override def write(kryo: Kryo, output: Output, v: DEL) = output.writeString(v.key)
    }
    class ADDSerializer extends Serializer[ADD] {
      override def read(kryo: Kryo, input: Input, t: Class[ADD]): ADD = ADD(input.readString(), kryo.readClassAndObject(input))
      override def write(kryo: Kryo, output: Output, v: ADD) = {
        output.writeString(v.key)
        kryo.writeClassAndObject(output, v.v);
      }
    }
  }

  {
    CmdSerialization.register(kryi)
  }

}

abstract class CMD
case class ADD(key: String, v: Any) extends CMD
case class DEL(key: String) extends CMD
case class LISTEN(key: String) extends CMD

class StoreConnection extends Connection

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

class StoreClient(port: Int) extends Store(port) {
  val client = new Client(8192, 2048, new KryoSerialization(kryi))

  val connect = () => {
    println("connecting");
    client.start();
    client.connect(500000, "localhost", port);
    client.addListener(new Listener {
      override def received(c: Connection, obj: Object) {
      }
    })
  }

}

object StoreServerTester {
  def main(args: Array[String]) {
    //	  val StoreServer = new StoreServer(1234)
    //	  StoreServer.accept()

    var listeners = new TreeMap[String, List[ScalaObject]]
    //var listeners = new 

    val client = new StoreClient(9999)

    client.connect()

    client.client.addListener(new Listener {
      override def connected(c: Connection) {
      }
      override def disconnected(c: Connection) {
      }
      override def received(c: Connection, obj: Object) {
        obj match {
          case c: CMD => c match {
            case ADD(key, v) => println(key + " " + v)
          }
          case _ =>
        }
      }
    })

    client.client.sendTCP(new LISTEN("bss"))
    client.client.sendTCP(new LISTEN("ass.1.3"))
    client.client.sendTCP(new ADD("bss.1.2.3.4", 45))
    client.client.sendTCP(new ADD("ass.1.2.3.4", 45))
    client.client.sendTCP(new ADD("ass.1.3.3.4", 45))
    client.client.sendTCP(new LISTEN("ass"))

  }
}