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
	
	public void payIn()
	{
		
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

		
		Connection con = DriverManager.getConnection("jdbc:mariadb://"
				+ "10.37.129.3:3306"
				+ "/?rewriteBatchedStatements=true","dbi", "dbi_pass");
		Statement statement = con.createStatement();
						
		// Eingabe initialisieren
		Scanner scanner = new Scanner(System.in);
		
		// Aufgabenstellung
		System.out.println("Aufgabenstellung: Praktikumgsaufgabe 9\n");
				
		// n abfragen
		System.out.print("Geben Sie den Parameter 'n' ein: ");
		String eingabe = scanner.nextLine();
		int queryInput = Integer.parseInt(eingabe);
		scanner.close();			

	}
}
