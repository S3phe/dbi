import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DBI_PT4_9 {

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
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
	    {
	    		result = rs.getInt(1);
	    		result += delta;
	    }
		txt = "update branches SET balance ="+result+" where branchid = "+branchId;
		stm.executeUpdate(txt);
		
		// UPDATE BALANCE@ TELLERS
		txt = "select balance from tellers where tellersid = "+tellersId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
	    {
	    		result = rs.getInt(1);
	    		result += delta;
	    }
		txt = "update tellers SET balance ="+result+" where tellersid = "+tellersId;
		stm.executeUpdate(txt);
		
		// UPDATE BALANCE@ ACCOUNTS
		txt = "select balance from accounts where accid = "+accId;
		stm.executeUpdate(txt);
		rs = stm.getResultSet();
		if( rs != null ) 
		{
			result = rs.getInt(1);
		  	result += delta;
		}
		txt = "update accounts SET balance ="+result+" where accid = "+accId;
		stm.executeUpdate(txt);
						
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
		Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/test","root", "dbi2015");
		Statement statement = con.createStatement();
						
		// Eingabe initialisieren
		Scanner scanner = new Scanner(System.in);
		
		// Aufgabenstellung
		System.out.println("Aufgabenstellung: Praktikumgsaufgabe 9\n");
				
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
}
