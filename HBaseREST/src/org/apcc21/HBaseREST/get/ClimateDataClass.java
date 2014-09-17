package org.apcc21.HBaseREST.get;

import java.util.ArrayList;
import java.util.List;

public class ClimateDataClass {
	public List<Row> Row;

	public List<Row> getRow(){
		return Row;
	}
	
	public class Row{
		public String key;
		public List<Cell> Cell;
				
		public String getKey(){
			return key;
		}
		
		public List<Cell> getCell(){
			return Cell;
		}
	}
	
	public class Cell{
		public String column;
		public String timestamp;
		public String $;
		
		public String getColumn(){
			return column;
		}
		
		public String getTimestamp(){
			return timestamp;
		}
		
		public String get$(){
			return $;
		}
	}
}