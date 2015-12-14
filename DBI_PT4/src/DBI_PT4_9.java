import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DBI_PT4_9 {
	// Eingabe initialisieren
	static Scanner scanner = new Scanner(System.in);
	
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
	
		//Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/test","root", "dbi2015");
		Connection con = DriverManager.getConnection("jdbc:mariadb://10.37.129.3:3306/benchmark","dbi", "dbi_pass");			
		
		System.out.println("Aufgabenstellung: Praktikumgsaufgabe 9\n");
		menu(con);
				
	}
	
	public static String kontostand(Connection con, Integer account) throws SQLException{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		String txt = "SELECT balance FROM accounts WHERE accid ="+account;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
	    while( rs != null ) 
	    {
	    		result = rs.getInt(1);
	    }
		return "Kontostand: "+result;
		
	}
	
	public static String einzahlung(Connection con, int accId, int tellersId, int branchId, int delta) throws SQLException
	{
		Statement stm = con.createStatement();
		ResultSet rs = null;
		int result = 0;
		
		// UPDATE BALANCE@ BRANCHES
		String txt = "SELECT balance FROM branches WHERE branchid = "+branchId;
		stm.execute(txt);
		rs = stm.getResultSet();
		
		if(rs!=null) 
	    {
	    		result = rs.findColumn("balance");
	    		result += delta;
	    }
		txt = "UPDATE branches SET balance ="+result+" WHERE branchid = "+branchId;
		stm.executeUpdate(txt);
		rs = null;
		// UPDATE BALANCE@ TELLERS
		txt = "SELECT balance FROM tellers WHERE tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
	    {
				result = rs.findColumn("balance");
	    		result += delta;
	    }
		txt = "UPDATE tellers SET balance ="+result+" WHERE tellerid = "+tellersId;
		stm.executeUpdate(txt);
		rs = null;
		// UPDATE BALANCE@ ACCOUNTS
		txt = "SELECT balance FROM accounts WHERE accid = "+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
		{
			result = rs.findColumn("balance");
		  	result += delta;
		}
		txt = "UPDATE accounts SET balance ="+result+" WHERE accid = "+accId;
		stm.executeUpdate(txt);
		rs = null;
		return "Erfolgreich.";	
	}
	
	public static String analyse(Connection con, Integer delta){
		return null;
		
		
	}
	
	public static void menu(Connection con) throws SQLException {
		while(true){
			System.out.println();
			System.out.println("1 Kontostands-TX");
			System.out.println("2 Einzahlungs-TX");
			System.out.println("3 Analyse-TX");
			System.out.println("4 Beenden");
			System.out.print("Eingabe: ");
			int auswahl = Integer.parseInt(scanner.nextLine());
			
			switch(auswahl){
				case 1: menu_kontostand(con); break;
				case 2: menu_einzahlung(con); break;
				case 3: menu_analyse(con); break;
				case 4: System.exit(0); break;
				default: menu(con); break;
			}
		}
	}
	
	
	public static void menu_kontostand(Connection con) throws SQLException{
		System.out.print("Kontonummer (ACCID); ");
		int accid = Integer.parseInt(scanner.nextLine());
		System.out.print(kontostand(con,accid));
	}
	
	public static void menu_einzahlung(Connection con) throws SQLException{
		System.out.print("Geben Sie eine AccountID ein: ");
		int accid = Integer.parseInt(scanner.nextLine());
		System.out.print("Geben Sie eine BranchID ein: ");
		
		int branchid = Integer.parseInt(scanner.nextLine());
		System.out.print("Geben Sie eine TellersID ein: ");
		int tellersid = Integer.parseInt(scanner.nextLine());
		System.out.print("Geben Sie ein Delta ein: ");
		int delta = Integer.parseInt(scanner.nextLine());	
		System.out.println(einzahlung(con, accid, tellersid, branchid, delta));
	}
	
	public static void menu_analyse(Connection con){
		System.out.print("Geben Sie den Differenzbetrag (Delta) ein: ");
		int delta = Integer.parseInt(scanner.nextLine());
		System.out.println(analyse(con,delta));
	}
	
	
}
