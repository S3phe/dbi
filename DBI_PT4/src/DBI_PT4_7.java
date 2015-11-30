
import java.sql.*;
import java.util.Scanner;
import java.util.Random;


public class DBI_PT4_7 {

	protected static Random rand = new Random();
	
	
	
	public static String randomString (int length) {
		String chars="ABCDEFGH0123456789";
		
		StringBuilder buf = new StringBuilder();
		for (int i=0; i<length; i++) {
			buf.append(chars.charAt(rand.nextInt(chars.length())));
		}
		return buf.toString();
	}
	protected static void create_database(Connection con) throws SQLException{
		Statement statement = con.createStatement(); 
		// Alte Datenbank löschen
		statement.execute("DROP DATABASE benchmark");
		// Datenbank erstellen
		statement.execute("CREATE DATABASE benchmark");
		statement.execute("USE benchmark");
		// Tabellen erstellen
		statement.execute("CREATE TABLE branches (branchid int not null, branchname char(20) not null, balance int not null, address char(72) not null, primary key (branchid));");
		statement.execute("CREATE TABLE accounts (accid int not null, name char(20) not null, balance int not null, branchid int not null, address char(68) not null, primary key (accid), foreign key (branchid) references branches (branchid));");
		statement.execute("CREATE TABLE tellers (tellerid int not null, tellername char(20) not null, balance int not null, branchid int not null, address char(68) not null, primary key (tellerid), foreign key (branchid) references branches (branchid));");
		statement.execute("CREATE TABLE history (accid int not null, tellerid int not null, delta int not null, branchid int not null, accbalance int not null, cmmnt char(30) not null, foreign key (accid) references accounts (accid), foreign key (tellerid) references tellers (tellerid),	foreign key (branchid) references branches (branchid));"); 
	}
	
	protected static void db_optimize(Connection con) throws SQLException{
		Statement sql = con.createStatement();
		sql.execute("SET FOREIGN_KEY_CHECKS=0");
		sql.execute("SET UNIQUE_CHECKS=0");
		con.setAutoCommit(false);
	}
	
	protected static void db_deoptimize(Connection con) throws SQLException{
		Statement sql = con.createStatement();
		sql.execute("SET FOREIGN_KEY_CHECKS=1");
		sql.execute("SET UNIQUE_CHECKS=1");
		con.setAutoCommit(true);
	}
	
	protected static void fill_branches(Statement statement, int n) throws SQLException{
		// Branches:  (n Tupel)
		// - BRANCHID (1 bis n)
		// - BALANCE = 0
		// - BRANCHNAME = (random 20 chars)
		// - ADDRESS = (random 72 chars)
		
		StringBuilder build = new StringBuilder("INSERT INTO branches (branchid, branchname, balance, address) VALUES ");
		String feld_branchname = randomString(20);
		String feld_address = randomString(72);

		for (int i=1;i<=n;i++){
			build.append("("+i+",'"+feld_branchname+"',0,'"+feld_address+"')");
			if (i<n){
				build.append(", ");
				
			}else{
				build.append(";");
			}
			
		}
		statement.execute(build.toString());
	}
	
	protected static void fill_accounts(Statement statement,int n) throws SQLException{
		// Accounts: (n * 100000 Tupel)
		// - ACCID (1 bis n*100000)
		// - NAME = (random 20 chars)
		// - BALANCE = 0
		// - BRANCHID = (random 1 bis n)
		// - ADDRESS = (random 68 chars)
		
		StringBuilder build = new StringBuilder("INSERT INTO accounts (accid, name, balance, branchid, address) VALUES ");
		

		int feld_branchid=0; 
		String feld_name = randomString(20);
		String feld_address = randomString(68);
		
		for (int i=1;i<=n*100000;i++){
			feld_branchid = rand.nextInt(n)+1;
			
			build.append("("+i+",'"+feld_name+"',0,"+feld_branchid+",'"+feld_address+"')");
			if (i<n*100000){
				build.append(", ");
				
			}else{
				build.append(";");
			}
		}
		statement.execute(build.toString());
	}
	
	protected static void fill_tellers(Statement statement,int n) throws SQLException{
		// Tellers: (n * 10 Tupel)
		// TELLERID = 1 bis n*10
		// TELLERNAME = (random 20 chars)
		// BALANCE = 0
		// BRANCHID = (random 1 bis n)
		// ADDRESS = (random 68 chars)
		

		StringBuilder build = new StringBuilder("INSERT INTO tellers (tellerid, tellername, balance, branchid, address) VALUES ");
		
		
		int feld_branchid= 0; 
		String feld_tellername=randomString(20);
		String feld_address = randomString(68);
		
		for (int i=1;i<=n*10;i++){
			feld_branchid= rand.nextInt(n)+1;
			build.append("("+i+",'"+feld_tellername+"',0,"+feld_branchid+",'"+feld_address+"')");	
			if (i<n*10){
				build.append(", ");
				
			}else{
				build.append(";");
			}
		}
		statement.execute(build.toString());
	}
	
	public static void main(String[] args) throws SQLException {
				// Datenbankverbindung herstellen
				Connection con = DriverManager.getConnection("jdbc:mariadb://10.37.129.3:3306/benchmark?rewriteBatchedStatements=true","dbi", "dbi_pass");
				
				
				
				
				// Eingabe initialisieren
				Scanner scanner = new Scanner(System.in);
				
				//Aufgabenstellung
				System.out.println("Aufgabenstellung: Praktikumgsaufgabe 7");
				System.out.println("Entwickeln Sie ein Programm, das einen Aufrufparameter n erwartet und eine ");
				System.out.println("initiale ntps-Datenbank auf dem gewählten Datenbankmanagementsystem erzeugt. ");
				System.out.println("---");
						
				// n abfragen
				System.out.print("Geben Sie den Parameter 'n' ein: ");
				String eingabe = scanner.nextLine();
				int n=Integer.parseInt(eingabe);
				scanner.close();
				
				// Datenbank neu erstellen
				create_database(con);
				db_optimize(con);			// Unsere Optimierungen anwenden
				
				
				// Batch-Statements initialisieren
				Statement statement=con.createStatement();
				long startTime_summe,startTime,runTime=0;
				startTime= System.currentTimeMillis();
				startTime_summe = startTime;
				fill_branches(statement, n);
				runTime=System.currentTimeMillis()-startTime;
				System.out.println("Branches: "+runTime+" ms ("+(runTime/1000)+" s)");
				
				System.gc(); // Empfehlung den Garbage Collector auszuführen, damit die RAM-Nutzung im Rahmen bleibt.
				
				startTime= System.currentTimeMillis();
				runTime=System.currentTimeMillis()-startTime;
				fill_accounts(statement, n);
				runTime=System.currentTimeMillis()-startTime;
				System.out.println("Accounts: "+runTime+" ms ("+(runTime/1000)+" s)");
				
				System.gc(); // Empfehlung den Garbage Collector auszuführen, damit die RAM-Nutzung im Rahmen bleibt.
				
				startTime= System.currentTimeMillis();
				fill_tellers(statement, n);
				runTime=System.currentTimeMillis()-startTime;
				System.out.println("Tellers: "+runTime+" ms ("+(runTime/1000)+" s)");
				runTime=System.currentTimeMillis()-startTime_summe;
				System.out.println("Summe: "+runTime+" ms ("+(runTime/1000)+" s)");
				
				con.commit();			// Alle offenen Vorgänge abschließen
				db_deoptimize(con);		// Optimierungen rückgängig machen

				System.out.println("Fertig.");
				
		
		
	}

}
