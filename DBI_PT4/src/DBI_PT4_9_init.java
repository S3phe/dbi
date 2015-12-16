import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class DBI_PT4_9_init {
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

	public static void db_prepare(Connection con) throws SQLException{
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
					build.append(",");
				}else{
					build.append(";");
				}
			}
			statement.execute(build.toString());
			build.delete(0,build.length());
			System.out.println("Fortschritt: "+((i+1)*10)+"%");
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
		System.out.println("Datenbank ist vorbereitet. Bereit fuer Benchmark.");	
	}
}
