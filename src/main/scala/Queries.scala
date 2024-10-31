object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT(Some(t), "FILTER", Not(Field("name", _ == "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] = {
    queryT(queryT(Some(queryDB(queryDB(Some(db), "CREATE", "Inserted Fellas"), "SELECT", List("Inserted Fellas")) match {
      case Some(database) => database.tables.head
      case None => Table("", List.empty)
    }), "INSERT", List(
      Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
      Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
      Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
      Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
    )), "SORT", "age")
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
    queryDB(Some(db), "SELECT", List("People", "Hobbies")) match {
      case Some(database) => queryDB(Some(database), "JOIN", "People", "name", "Hobbies", "name") match {
        case Some(database2) => queryT(
          queryT(
            queryT(
              queryT(
                Some(database2.tables.head), "FILTER", Not(Field("age", _ == ""))), 
              "FILTER", Not(Field("hobby", _ == ""))), 
            "FILTER", Compound(_ && _, List(
                  Field("age", _.toInt < 25),
                  Field("name", _.startsWith("J")),
                ))), 
          "EXTRACT", List("name", "hobby"))
        case None => Some(Table("", List.empty))
      }
      case None => Some(Table("", List.empty))
    }
  }
}
