class City(n: String, c: String) extends Serializable {

  def name: String = n
  def country: String = c

  override def toString: String = name + " (" + country + ")"

  def equals (c: City): Boolean = {
    if(this.name.equals(c.name) && this.country.equals(c.country)) true
    else false
  }

}

class CompleteCity(n:String, c:String, lat: Double, long: Double) extends City(n,c){

  def latitude = lat
  def longitude = long

  override def toString: String = name + " (" + country + ") - Coordinate: " + latitude + " - " + longitude

}

