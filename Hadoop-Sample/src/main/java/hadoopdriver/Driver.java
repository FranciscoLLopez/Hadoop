package hadoopdriver;

import hadoop.friends.FriendsMRWritables;
import hadoop.histograma.HistogramaFlow;
import aux.ProgramDriver;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
	    addClass("HistogramaFlow", HistogramaFlow.class, "Calcula el min y el max");
	    addClass("FriendsMRWritables", FriendsMRWritables.class, "Calcula el algoritmo Los amigos de mis amigos");
	}

	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
