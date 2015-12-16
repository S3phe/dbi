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
	
	
	// TEST VON SEBASTIAN
	
	
	
	public static Random rand = new Random();
	
	public static String randomString (int length) {
		
		String chars="ABCDEFGH0123456789";
		StringBuilder buf = new StringBuilder();
		for (int i=0; i<length; i++) {
			buf.append(chars.charAt(rand.nextInt(chars.length())));
		}
		return buf.toString();
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
	
	public static void benchmark(Connection con) throws SQLException {
		System.out.println("Statement erzeugen...");
		
		
		// Komplette Benchmark Datenbank neu erstellen
		Statement statement=con.createStatement();
		
		// Bestehende Datenbank löschen, falls diese existiert und neu anlegen
		System.out.println("Datenbank löschen und neu anlegen...");
		statement.execute("DROP DATABASE IF EXISTS benchmark");
		statement.execute("CREATE DATABASE benchmark");
		statement.execute("USE benchmark");
		// Tabellen gemäß Vorgabe anlegen
		System.out.println("Tabellen erzeugen...");
		statement.execute("CREATE TABLE branches (branchid int not null, branchname char(20) not null, balance int not null, address char(72) not null, primary key (branchid));");
		statement.execute("CREATE TABLE accounts (accid int not null, name char(20) not null, balance int not null, branchid int not null, address char(68) not null, primary key (accid), foreign key (branchid) references branches (branchid));");
		statement.execute("CREATE TABLE tellers (tellerid int not null, tellername char(20) not null, balance int not null, branchid int not null, address char(68) not null, primary key (tellerid), foreign key (branchid) references branches (branchid));");
		statement.execute("CREATE TABLE history (accid int not null, tellerid int not null, delta int not null, branchid int not null, accbalance int not null, cmmnt char(30) not null, foreign key (accid) references accounts (accid), foreign key (tellerid) references tellers (tellerid), foreign key (branchid) references branches (branchid));"); 
		
		System.out.println("Datenbank optimieren");
		db_optimize(con);
		// 100 tps-Datenbank mit Inhalt füllen
		Integer n = 100;
		System.out.println("Tabellen mit Inhalt füllen... ("+n+"tps)");
		
		System.out.println("Fill Branches...");
		// Fill Branches
		StringBuilder build = new StringBuilder();
		build.append("INSERT INTO branches (branchid, branchname, balance, address) VALUES ");
		String branchname = randomString(20);
		String address = randomString(72);

		for (int i=1;i<=n;i++){
			build.append("("+i+",'"+branchname+"',0,'"+address+"')");
			if (i<n){
				build.append(", ");
				
			}else{
				build.append(";");
			}
			
		}
		statement.execute(build.toString());
		build.delete(0,build.length());
		System.out.println("Fill Accounts...");
		
		int 	branchid=0; 
		String 	name = randomString(20);
		address = randomString(68);
		int accid=0;
		
		// Daten splitten, da RAM der VM (DBMS) nicht ausreicht
		
		// Angabe, wieviele Werte in einen String sollen:
		int inserts_pro_durchgang = 1000000;
		int insert_durchgaenge = (n*100000) / inserts_pro_durchgang;

		for (int i=0;i<insert_durchgaenge;i++)
		{	
			build.append("INSERT INTO accounts (accid, name, balance, branchid, address) VALUES ");
			for (int j=1;j<=inserts_pro_durchgang;j++)
			{
				branchid = rand.nextInt(n)+1;
				accid=i*inserts_pro_durchgang + j;
				build.append("("+accid+",'"+name+"',0,"+branchid+",'"+address+"')");
				if (j<inserts_pro_durchgang){
					build.append(", ");
				}else{
					build.append(";");
				}
			}
			statement.execute(build.toString());
			build.delete(0,build.length());
			System.out.println("Fortschritt: "+i+"/"+insert_durchgaenge);
		}
		
		build.delete(0,build.length());
		System.out.println("Fill Tellers...");
		
		build.append("INSERT INTO tellers (tellerid, tellername, balance, branchid, address) VALUES ");
		
		branchid= 0; 
		String 	tellername=randomString(20);
		address = randomString(68);
		
		for (int i=1;i<=n*10;i++){
			branchid= rand.nextInt(n)+1;
			build.append("("+i+",'"+tellername+"',0,"+branchid+",'"+address+"')");	
			if (i<n*10){
				build.append(", ");
				
			}else{
				build.append(";");
			}
		}
		statement.execute(build.toString());
		
		con.commit();
		System.out.println("Datenbank deoptimieren...");
		db_deoptimize(con);
		System.out.println("Datenbank ist vorbereitet. Bereit für Benchmark.");
		
		
		
		
	}
	
	
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
				txy
			}
			
			while(runtime<10%)
			{
				txy
			}
			
			while(runTime<40%)
			{
				txyz
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
		//Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3307/benchmark","root", "dbi2015");
		Connection con = DriverManager.getConnection("jdbc:mariadb://10.37.129.3:3306/benchmark","dbi", "dbi_pass");			
		menu(con);
	}
}
	
