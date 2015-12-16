import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Scanner;

public class DBI_PT4_9 {
	// Eingabe initialisieren
	static Scanner scanner = new Scanner(System.in);
	
	protected static Random rand = new Random();
	
	public static void txLoop()
	{
		// Zeitmessung starten
		long fasenTime,runTime,completeTime,txTime=0;
		
		fasenTime = System.currentTimeMillis();
		completeTime= System.currentTimeMillis();
		txTime = System.currentTimeMillis();
		
		while(fasenTime < einschwingfase)
		{
			while(runTime<35%)
			{
				tx_kontostand(con,);
			}
			
			while(runtime<10%)
			{
				tx_einzahlung(con);
			}
			
			while(runTime<40%)
			{
				tx_analyse(con);
			}
		}
		
		fasenTime = System.currentTimeMillis();
		while(fasenTime < messfase)
		{
			while(runTime<35%)
			{
				tx_kontostand(con);
			}
			
			while(runtime<10%)
			{
				tx_einzahlung(con);
			}
			
			while(runTime<40%)
			{
				tx_analyse(con);
			}
		}
		
		fasenTime = System.currentTimeMillis();
		while(fasenTime < ausschwingfase)
		{
			while(runTime<35%)
			{
				tx_kontostand(con);
			}
			
			while(runtime<10%)
			{
				tx_einzahlung(con);
			}
			
			while(runTime<40%)
			{
				tx_analyse(con);
			}
		}
			
		runTime=System.currentTimeMillis()-startTime;
		System.out.println("Fertig. (Laufzeit: "+runTime+" ms)");
	}
		
	public static void resume() throws IOException
	{
		String dummy = scanner.nextLine();
	}
	
	// HILFSFUNKTIONEN //
	// Hilfsfunktion zur Ermittlung eines Kontostandes anhand seiner ACCOUNT-ID
	public static int getBalance(Connection con, int accId) throws SQLException
	{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		String txt = "SELECT balance FROM accounts WHERE accid ="+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
	    if( rs.next())
	    {
	    		result = rs.getInt(1);
	    }
		return result;
	}
	
	// Hilfsfunktion zur Einzahlung mit einem speziellen Wert DELTA
	public static int payIn(Connection con, int accId, int tellersId, int branchId, int delta) throws SQLException
	{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		int col = 0;
		
		// UPDATE BALANCE@ BRANCHES
		String txt = "SELECT balance FROM branches WHERE branchid = "+branchId;
		stm.execute(txt);
		rs = stm.getResultSet();
		
		if(rs.next()) 
	    {
				col = rs.findColumn("balance");
				result = Integer.parseInt(rs.getString(col));
				result += delta;
	    }
		txt = "UPDATE branches SET balance ="+result+" WHERE branchid = "+branchId;
		stm.executeUpdate(txt);
		rs = null;
		
		// UPDATE BALANCE@ TELLERS
		txt = "SELECT balance FROM tellers WHERE tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs.next()) 
	    {
				col = rs.findColumn("balance");
				result = Integer.parseInt(rs.getString(col));
	    		result += delta;
	    }
		txt = "UPDATE tellers SET balance ="+result+" WHERE tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = null;
	
		// UPDATE BALANCE@ ACCOUNTS
		txt = "SELECT balance FROM accounts WHERE accid = "+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs.next()) 
		{
			col = rs.findColumn("balance");
			result = Integer.parseInt(rs.getString(col));
		  	result += delta;
		}
		txt = "UPDATE accounts SET balance ="+result+" WHERE accid = "+accId;
		stm.executeUpdate(txt);
		txt = "INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES ('"+accId+"','"+tellersId+"','"+delta+"','"+branchId+"','"+result+"', 'Einzahlung')";
		stm.execute(txt);
		return result;
	}
	
	// Hilfsfunktion zur Ueberpruefung bereits getaetigter Zahlungen mit einem speziellen Wert DELTA
	public static int consistsDelta(Connection con, int delta) throws SQLException{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		
		// ALLE TUPEL AUS HISTORY MIT DEM UEBERGABEPARAM. DELTA
		String txt = "SELECT * FROM history WHERE delta = "+delta;
		stm.execute(txt);
		rs = stm.getResultSet();
		int counter = 0;
		while(rs.next()) 
	    {
				counter+=1;
	    }
		return counter;	
	}
	
	// Menuefuehrung
		public static void menu(Connection con) throws SQLException, IOException {
			while(true)
			{
				// Aufgabenstellung
				System.out.println("Aufgabenstellung: Praktikumgsaufgabe 9\n");
				// Auswahl
				System.out.println("\n\n\n\n\n\n");
				System.out.println("///////   MENU   ///////");
				System.out.println("------------------------");
				System.out.println("||||||||||||||||||||||||");
				System.out.println("------------------------");
				System.out.println("| - 1: KONTOSTAND-TX   |");
				System.out.println("|----------------------|");
				System.out.println("| - 2: EINZAHLUNG-TX   |");
				System.out.println("|----------------------|");
				System.out.println("| - 3: ANALYSE-TX      |");
				System.out.println("|--------------------- |");
				System.out.println("| - 4. BEENDEN         |");
				System.out.println("|----------------------|");
				System.out.print("Eingabe: ");
				int auswahl = Integer.parseInt(scanner.nextLine());
				
				
				switch(auswahl){
					case 1: tx_kontostand(con); break;
					case 2: tx_einzahlung(con); break;
					case 3: tx_analyse(con); break;
					case 4: System.exit(0); break;
					default: menu(con); break;
				}	
			}
		}
		
		public static void tx_kontostand(Connection con) throws SQLException, IOException{
			System.out.println("-- Abfragen des Kontostands --\n\n");
			System.out.print("Geben Sie eine AccountID ein: ");
			String acc = scanner.nextLine();
			int accid = Integer.parseInt(acc);
			int output = getBalance(con, accid);
			scanner.close();
			System.out.println("Der momentane Kontostand des Konto's "+accid+" betraegt "+output+" Einheiten!");
			resume();
		}
		
		public static void tx_einzahlung(Connection con) throws SQLException, IOException{
			System.out.println("-- Geld einzahlen --\n\n");
			System.out.print("Geben Sie eine AccountID ein: ");
			String acc = scanner.nextLine();
			int accid = Integer.parseInt(acc);
			System.out.print("Geben Sie eine BranchID ein: ");
			String branch = scanner.nextLine();
			int branchid = Integer.parseInt(branch);
			System.out.print("Geben Sie eine TellersID ein: ");
			String tellers = scanner.nextLine();
			int tellersid = Integer.parseInt(tellers);
			System.out.print("Geben Sie ein Delta ein: ");
			String value = scanner.nextLine();
			int delta = Integer.parseInt(value);
			scanner.close();			
			int output = payIn(con, accid, tellersid, branchid, delta);
			System.out.println("Der aktualisierte Kontostand des Konto's "+accid+" betraegt "+output+" Einheiten!");
			resume();
		}
		
		public static void tx_analyse(Connection con) throws SQLException, IOException{
			System.out.println("-- Geldeinzahlungen bereits vorhanden? --\n\n");
			System.out.print("Geben Sie ein Delta ein: ");
			String s = scanner.nextLine();		
			int delta = Integer.parseInt(s);
			int amount = consistsDelta(con, delta);
			System.out.println("Die Anzahl der Ueberweisungen mit dem Delta-Wert "+delta+" betragen: "+amount);
			resume();
		}
	
	// Mainfunktion mit Aufruf des Menue's (enthaelt Steuerung des Programmfluss) //	
	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3307/benchmark","root", "dbi2015");
		//Connection con = DriverManager.getConnection("jdbc:mariadb://10.37.129.3:3306/benchmark","dbi", "dbi_pass");			
		menu(con);
	}
}
	
