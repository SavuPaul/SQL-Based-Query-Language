case class Database(tables: List[Table]) {
  override def toString: String = {
    val listTables: List[String] = tables.map(table => table.toString)
    listTables.mkString("\n\n")
  }

  def create(tableName: String): Database = {
    // determines whether tableName already exists or not
    val tableExists: Boolean = tables.exists(table => table.name == tableName)

    // adds the table to the database if it doesn't exist
    if (tableExists) Database(tables)
    else Database(tables :+ Table(tableName, List.empty))
  }

  def drop(tableName: String): Database = {
    // determines whether tableName already exists or not
    val tableExists: Boolean = tables.exists(table => table.name == tableName)

    // if table does not exist, return the already existing database
    if (!tableExists) Database(tables)
    else {
      // recreate the list of tables from the database
      val newTables: List[Table] = tables.foldLeft(List.empty)((acc, elem) => {
        if (elem.name != tableName) {
          elem :: acc
        }
        else acc
      })
      Database(newTables.reverse)
    }
  }

  // finds a specific table in the db and returns it
  def findTable(crtTable: String): Table = {
    tables.filter(table => table.name == crtTable).head
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    // firstly, verify if all tableNames exist in the database
    def tableExists(tableName: String): Boolean = {
      tables.exists(table => table.name == tableName)
    }

    // check if any of the table names do not exist
    val existingTables: List[Boolean] = tableNames.map(tableName => tableExists(tableName))
    val check: List[Boolean] = existingTables.filter(value => !value)

    // if the list contains at least one false, return None
    if (check.nonEmpty) None
    else {
      // create a list of the tables from the tableNames list
      val selectedTables: List[Table] = tableNames.foldLeft(List.empty)((acc, elem) => {
        val tableAdd: Table = findTable(elem)
        acc :+ tableAdd
      })
      Some(Database(selectedTables))
    }
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    // gets the tables from the database
    val t1: Table = findTable(table1)
    val t2: Table = findTable(table2)

    // returns true if a pair (key -> value) exists in a table, else false
    def containsKeyValue(table: Table, key: String, value: String): Boolean = {
      table.data.exists(row => row(key) == value)
    }

    // creates a map where the keys are represented by a table header and the values are empty
    def createRow(table: Table): Row = {
      val header: List[String] = table.header
      header.foldLeft(Map.empty)((acc, elem) => {
        acc + (elem -> "")
      })
    }

    // extension to add ";" between fields where both values exist
    // also filters elements to that c1 and c2 appear only once despite having different names
    extension (map1: Row) {
      def +++(map2: Row): Row = {
        val newMap = map1 ++ map2.map((key, value) => {
          if (map1.contains(key) && map1.get(key) != map2.get(key)) {
            key -> (map1(key) + ";" + value)
          } else {
            key -> value
          }
        })
        if (c1 != c2) newMap.filter(elem => elem._1 != c2)
        else newMap
      }
    }

    // gets the elements which exist in both tables
    val joinedTable1: Tabular = t1.data.flatMap(row1 => {
      val value1 = row1.get(c1)
      t2.data.flatMap(row2 => {
        val value2 = row2.get(c2)
        if (value1 == value2) {
          row1 +++ row2 :: Nil
        } else {
          Nil
        }
      })
    })

    // get the elements from table1 which are not found in table2
    val notIn2: Tabular = t1.data.flatMap(row1 => {
      if (!containsKeyValue(t2, c2, row1(c1))) {
        val defaultRow = createRow(t2)
        val combinedMaps = defaultRow ++ row1
        if (c1 != c2) combinedMaps.filter(elem => elem._1 != c2) :: Nil
        else combinedMaps :: Nil
      } else {
        Nil
      }
    })

    // get the elements from table2 which are not found in table1
    val notIn1: Tabular = t2.data.flatMap(row2 => {
      if (!containsKeyValue(t1, c1, row2(c2))) {
        val defaultRow = createRow(t1)
        val value = row2(c2)
        val combinedMaps = defaultRow ++ (row2 - c2)
        combinedMaps + (c1 -> value) :: Nil
      } else {
        Nil
      }
    })

    // concatenate all tables
    val joinTable: Table = Table(table1, joinedTable1 ++ notIn2 ++ notIn1)

    // return the final joinTable
    Some(joinTable)
  }

  // Implement indexing here
  def apply(idx: Int): Table = {
    tables(idx)
  }
}
