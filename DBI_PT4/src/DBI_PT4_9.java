import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DBI_PT4_9 {
	// Eingabe initialisieren
	static Scanner scanner = new Scanner(System.in);
	
	
	public int getBalance(Connection con, int accId) throws SQLException
	{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		String txt = "select balance from accounts where accid ="+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
	    while( rs != null ) 
	    {
	    		result = rs.getInt(1);
	    }
		return result;
	}
	
	public static void payIn(Connection con, int accId, int tellersId, int branchId, int delta) throws SQLException
	{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		
		// UPDATE BALANCE@ BRANCHES
		String txt = "select balance from branches where branchid = "+branchId;
		stm.execute(txt);
		rs = stm.getResultSet();
		
		if(rs!=null) 
	    {
	    		result = rs.findColumn("balance");
	    		result += delta;
	    }
		txt = "update branches SET balance ="+result+" where branchid = "+branchId;
		stm.executeUpdate(txt);
		rs = null;
		// UPDATE BALANCE@ TELLERS
		txt = "select balance from tellers where tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
	    {
				result = rs.findColumn("balance");
	    		result += delta;
	    }
		txt = "update tellers SET balance ="+result+" where tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = null;
		// UPDATE BALANCE@ ACCOUNTS
		txt = "select balance from accounts where accid = "+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
		{
			result = rs.findColumn("balance");
		  	result += delta;
		}
		txt = "update accounts SET balance ="+result+" where accid = "+accId;
		stm.executeUpdate(txt);
		rs = null;	
	}
	
	public static void menu(Connection con) throws SQLException {
		// Aufgabenstellung
		System.out.println("Aufgabenstellung: Praktikumgsaufgabe 9\n");
		// Auswahl
		System.out.println();
		System.out.println("1 Kontostands-TX");
		System.out.println("2 Einzahlungs-TX");
		System.out.println("3 Analyse-TX");
		System.out.println("4 Beenden");
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
	
	
	public static void tx_kontostand(Connection con){
		
	}
	
	public static void tx_einzahlung(Connection con) throws SQLException{
		// n abfragen
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
		payIn(con, accid, tellersid, branchid, delta);
	}
	
	public static void tx_analyse(Connection con){
		
	}
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		// Hier kommt Aufgabe9 rein
		
		// Kontostands-TX (ACCID eingeben und BALANCE zurückgeben
		// Einzahlungs-TX (ACCID,TELLERID,BRANCHID,DELTA eingeben. 
		//	-	In "Branches" die Bilanzsumme Balance passend zur Branchid aktualisieren
		//	-	In "Tellers" die Bilanzsumme Balance passend zur Tellerid aktualisieren
		//	-	In "Accounts" den Kontostand BALANCE passend zur ACCID aktualisieren
		//	-		
		//Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/test","root", "dbi2015");
		Connection con = DriverManager.getConnection("jdbc:mariadb://10.37.129.3:3306/benchmark","dbi", "dbi_pass");
		Statement statement = con.createStatement();
						
		
		
		menu(con);
		
		
				
		
	}
}
