type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {
    val rows: List[String] = data.map(row => row.values.mkString(","))
    val tableString: String = header.mkString(",") + "\n" + rows.mkString("\n")
    tableString
  }

  // insert a row
  def insert(row: Row): Table = {
    val condition = data.exists(line => line.equals(row))
    if (condition) Table(name, data)
    else Table(name, data ::: List(row))
  }

  // delete a certain row
  def delete(row: Row): Table = {
    val newTable: Tabular = data.filter(line => line != row)
    Table(name, newTable)
  }

  // sort by a given column
  def sort(column: String): Table = {
    val sortTableData: Tabular = data.sortBy(line => line.get(column))
    Table(name, sortTableData)
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val line: Table = filter(f)

    val updatedData: Tabular = data.map { row =>
      if (line.data.contains(row)) {
        // if the row matches the filtered line
        row.map { case (key, value) =>
          key -> updates.getOrElse(key, value)
        }
      } else {
        // no modifications
        row
      }
    }

    Table(name, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredTable = data.filter(row => f.eval(row) match {
      case Some(true) => true
      case _ => false
    })
    Table(name, filteredTable)
  }

  def selectHelper(column: String): Table = {
    val col: Tabular = data.map(row => row.filter(elem => elem._1 == column))
    Table(name, col)
  }

  def select(columns: List[String]): Table = {
    // list of tables with the corresponding columns
    val tableList: List[Table] = columns.map(column => selectHelper(column))

    // get the data from the tables
    val tableDataList: List[Tabular] = tableList.map(_.data)

    // transpose it to become columns
    val bigTable: Tabular = tableDataList.transpose.map(_.flatten.toMap)
    Table(name, bigTable)
  }

  def header: List[String] = {
    tableData match {
      case Nil => List.empty
      case row :: _ => row.keys.toList
    }
  }

  def data: Tabular = tableData

  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    // splits at newline to create a new list, splits
    // at "," to create a new element for the inner list
    val tableAsList: List[List[String]] =
      s.split("\n").map(elem => elem.split(",").toList).toList

    // zip every element in the tail of tableAsList with the corresponding element
    // from the head of tableAsList, then map them to produce the data
    val newData = tableAsList.tail.map(row => tableAsList.head.zip(row).toMap)

    Table(name, newData)
  }
}

extension (table: Table) {
  def apply(idx: Int): Row = {
    table.data(idx)
  }
}
