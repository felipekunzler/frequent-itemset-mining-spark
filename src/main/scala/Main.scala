
object Main {

  def main(args: Array[String]): Unit = {

    val evenVal = (a: Int) => {
      println("evenVal")
      a % 2 == 0
    }

    println(evenVal(9))
    println(evenVal(10))

    def evenDef: Int => Boolean = {
      println("evenDef")
      a: Int => a % 2 == 0
    }
    println(evenDef(9))
    println(evenDef(10))

  }

  def fun(f: (Int, Int) => Int) {
    print(f(1, 2))
  }

}
