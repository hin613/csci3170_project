import java.util.Scanner;
import java.util.Calendar;
import java.sql.*;
import java.io.*;

public class CSCI3170Proj {

	//public static String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2312/db00";
    public static String dbAddress = "jdbc:mysql://localhost/csci3170";
	//public static String dbUsername = "Group00";
    public static String dbUsername = "root";
	//public static String dbPassword = "CSCI3170";
    public static String dbPassword = "";

	public static Connection connectToOracle(){
		Connection con = null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
		} catch (ClassNotFoundException e){
			System.out.println("[Error]: Java MySQL DB Driver not found!!");
			System.exit(0);
		} catch (SQLException e){
			System.out.println(e);
		}
		return con;
	}

	public static void createTables(Connection mySQLDB) throws SQLException{
		String NEASQL = "CREATE TABLE NEA (";
		NEASQL += "NID VARCHAR(10) NOT NULL,";
		NEASQL += "Distance DOUBLE NOT NULL,";
		NEASQL += "Family VARCHAR(6) NOT NULL,";
        NEASQL += "Duration INT(3) NOT NULL,";
        NEASQL += "Energy DOUBLE NOT NULL,";
        NEASQL += "PRIMARY KEY(NID))";
        //NEASQL += "Rtype VARCHAR(2),";
        //NEASQL += "PRIMARY KEY(NID),";
        //NEASQL += "FOREIGN KEY (Rtype) REFERENCES Resource(Rtype))";
        
        String resourceSQL = "CREATE TABLE Resource (";
		resourceSQL += "Rtype VARCHAR(2) NOT NULL,";
		resourceSQL += "Density DOUBLE NOT NULL,";
		resourceSQL += "Value DOUBLE NOT NULL,";
		resourceSQL += "PRIMARY KEY(Rtype))";
        
		String containSQL = "CREATE TABLE Contain (";
		containSQL += "NID VARCHAR(10) NOT NULL,";
		containSQL += "Rtype VARCHAR(2),";
		containSQL += "PRIMARY KEY(NID),";
		containSQL += "FOREIGN KEY (NID) REFERENCES NEA(NID),";
		containSQL += "FOREIGN KEY (Rtype) REFERENCES Resource(Rtype))";
        
        String spacecraftSQL = "CREATE TABLE SpacecraftModel (";
        spacecraftSQL += "Agency VARCHAR(4) NOT NULL,";
        spacecraftSQL += "MID VARCHAR(4) NOT NULL,";
        spacecraftSQL += "Num INT(2) NOT NULL,";
        spacecraftSQL += "Charge INT(5) NOT NULL,";
        spacecraftSQL += "Duration INT(3) NOT NULL,";
        spacecraftSQL += "Energy DOUBLE NOT NULL,";
        spacecraftSQL += "PRIMARY KEY(Agency, MID))";
        
        String amodelSQL = "CREATE TABLE A_Model (";
        amodelSQL += "Agency VARCHAR(4) NOT NULL,";
        amodelSQL += "MID VARCHAR(4) NOT NULL,";
        amodelSQL += "Num INT(2) NOT NULL,";
        amodelSQL += "Charge INT(5) NOT NULL,";
        amodelSQL += "Duration INT(3) NOT NULL,";
        amodelSQL += "Energy DOUBLE NOT NULL,";
        amodelSQL += "Capacity INT(2),";
        amodelSQL += "PRIMARY KEY(Agency, MID),";
        amodelSQL += "FOREIGN KEY (Agency,MID) REFERENCES SpacecraftModel(Agency,MID))";
        
        String rentalSQL = "CREATE TABLE RentalRecord (";
        rentalSQL += "Agency VARCHAR(4) NOT NULL,";
        rentalSQL += "MID VARCHAR(4) NOT NULL,";
        rentalSQL += "SNum INT(2) NOT NULL,";
        rentalSQL += "CheckoutDate DATE NOT NULL,";
        rentalSQL += "ReturnDate DATE,";
        rentalSQL += "PRIMARY KEY(Agency, MID, SNum),";
        rentalSQL += "FOREIGN KEY (Agency,MID) REFERENCES SpacecraftModel(Agency,MID))";

		Statement stmt  = mySQLDB.createStatement();
		System.out.println("Processing...");

		System.err.println("Creating Resource Table.");
		stmt.execute(resourceSQL);
        
        System.err.println("Creating SpacecraftModel Table.");
        stmt.execute(spacecraftSQL);

		System.err.println("Creating NEA Table.");
		stmt.execute(NEASQL);
		
		System.err.println("Creating Contain Table.");
		stmt.execute(containSQL);
        
        System.err.println("Creating A_Model Table.");
        stmt.execute(amodelSQL);
        
        System.err.println("Creating RentalRecord Table.");
        stmt.execute(rentalSQL);

		System.out.println("Done! Database is initialized!");
		stmt.close();
	}

	public static void deleteTables(Connection mySQLDB) throws SQLException{
		Statement stmt  = mySQLDB.createStatement();
		System.out.println("Processing...");
		stmt.execute("SET FOREIGN_KEY_CHECKS = 0;");
		stmt.execute("DROP TABLE IF EXISTS NEA");
		stmt.execute("DROP TABLE IF EXISTS Resource");
		stmt.execute("DROP TABLE IF EXISTS Contain");
		stmt.execute("DROP TABLE IF EXISTS SpacecraftModel");
		stmt.execute("DROP TABLE IF EXISTS A_Model");
        stmt.execute("DROP TABLE IF EXISTS RentalRecord");
		stmt.execute("SET FOREIGN_KEY_CHECKS = 1;");
		System.out.println("Done! Database is removed!");
		stmt.close();
	}

	public static void loadTables(Scanner menuAns, Connection mySQLDB) throws SQLException{

		String resourceSQL = "INSERT INTO Resource (RType, Density, Value) VALUES (?,?,?)";
		String NEASQL = "INSERT INTO NEA (NID, Distance, Family, Duration, Energy) VALUES (?,?,?,?,?)";
        String containSQL = "INSERT INTO Contain (NID, RType) VALUES (?,?)";
		String spacecraftSQL = "INSERT INTO SpacecraftModel (Agency, MID, Num, Charge, Duration, Energy) VALUES (?,?,?,?,?,?)";
		String amodelSQL = "INSERT INTO A_Model (Agency, MID, Num, Charge, Duration, Energy, Capacity) VALUES (?,?,?,?,?,?,?)";
		String rentalSQL = "INSERT INTO RentalRecord (Agency, MID, SNum, CheckoutDate, ReturnDate) VALUES (?,?,?,STR_TO_DATE(?,'%d-%m-%Y'),STR_TO_DATE(?,'%d-%m-%Y'))";

		String filePath = "";
		String targetTable = "";

		while(true){
			System.out.println("");
			System.out.print("Type in the Source Data Folder Path: ");
			filePath = menuAns.nextLine();
			if((new File(filePath)).isDirectory()) break;
		}

		System.out.println("Processing...");
		System.err.println("Loading resources.txt");
		try{
			PreparedStatement stmt = mySQLDB.prepareStatement(resourceSQL);
			String line = null;
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath+"/resources.txt"));
            line = dataReader.readLine();

			while ((line = dataReader.readLine()) != null) {
				String[] dataFields = line.split("\t");

                stmt.setString(1, dataFields[0]);
                stmt.setDouble(2, Double.parseDouble(dataFields[1]));
                stmt.setDouble(3, Double.parseDouble(dataFields[2]));
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.close();
		}catch (Exception e){
			System.out.println(e);
		}

		System.err.println("Loading neas.txt");
		try{
			PreparedStatement stmt = mySQLDB.prepareStatement(NEASQL);
            PreparedStatement stmt2 = mySQLDB.prepareStatement(containSQL);
			String line = null;
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath+"/neas.txt"));
            line = dataReader.readLine();

			while ((line = dataReader.readLine()) != null) {
				String[] dataFields = line.split("\t");
                
                stmt.setString(1, dataFields[0]);
                stmt.setDouble(2, Double.parseDouble(dataFields[1]));
                stmt.setString(3, dataFields[2]);
                stmt.setInt(4, Integer.parseInt(dataFields[3]));
                stmt.setDouble(5, Double.parseDouble(dataFields[4]));
				stmt.addBatch();
                stmt2.setString(1, dataFields[0]);
                if(!dataFields[5].equals("null")){
                    stmt2.setString(2, dataFields[5]);
                }
                else{
                    stmt2.setNull(2, Types.INTEGER);
                }
                stmt2.addBatch();
			}
			stmt.executeBatch();
            stmt2.executeBatch();
			stmt.close();
            stmt2.close();
		}catch (Exception e){
			System.out.println(e);
		}

		System.err.println("Loading spacecrafts.txt");
		try{
			PreparedStatement stmt = mySQLDB.prepareStatement(spacecraftSQL);
            PreparedStatement stmt2 = mySQLDB.prepareStatement(amodelSQL);

			String line = null;
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath+"/spacecrafts.txt"));
            line = dataReader.readLine();

			while ((line = dataReader.readLine()) != null) {
				String[] dataFields = line.split("\t");
                
                stmt.setString(1, dataFields[0]);
                stmt.setString(2, dataFields[1]);
                stmt.setInt(3, Integer.parseInt(dataFields[2]));
                stmt.setInt(4, Integer.parseInt(dataFields[7]));
                stmt.setInt(5, Integer.parseInt(dataFields[5]));
                stmt.setDouble(6, Double.parseDouble(dataFields[4]));
                stmt.addBatch();
                if(dataFields[3].equals("A")){
                    stmt2.setString(1, dataFields[0]);
                    stmt2.setString(2, dataFields[1]);
                    stmt2.setInt(3, Integer.parseInt(dataFields[2]));
                    stmt2.setInt(4, Integer.parseInt(dataFields[7]));
                    stmt2.setInt(5, Integer.parseInt(dataFields[5]));
                    stmt2.setDouble(6, Double.parseDouble(dataFields[4]));
                    stmt2.setInt(7, Integer.parseInt(dataFields[6]));
                    stmt2.addBatch();
                }
			}

			stmt.executeBatch();
            stmt2.executeBatch();
			stmt.close();
            stmt2.close();
		}catch (Exception e){
			System.out.println(e);
		}

		System.err.println("Loading rentalrecords.txt");
		try{
			PreparedStatement stmt = mySQLDB.prepareStatement(rentalSQL);
			String line = null;
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath+"/rentalrecords.txt"));
            line = dataReader.readLine();

			while ((line = dataReader.readLine()) != null) {
				String[] dataFields = line.split("\t");
                
                stmt.setString(1, dataFields[0]);
                stmt.setString(2, dataFields[1]);
                stmt.setInt(3, Integer.parseInt(dataFields[2]));
                stmt.setString(4, dataFields[3]);
                if(!dataFields[4].equals("null")){
                    stmt.setString(5, dataFields[4]);
                }
                else{
                    stmt.setNull(5, Types.INTEGER);
                }
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.close();
		}catch (Exception e){
			System.out.println(e);
		}

		System.out.println("Done! Data is inputted to the database!");
	}

	public static void showTables(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String[] table_name = {"Resource", "NEA", "Contain", "SpacecraftModel", "A_Model", "RentalRecord"};

		System.out.println("Number of records in each table:\n");
		for (int i = 0; i < 6; i++){
			Statement stmt  = mySQLDB.createStatement();
			ResultSet rs = stmt.executeQuery("select count(*) from "+table_name[i]);

			rs.next();
			System.out.println(table_name[i]+": "+rs.getString(1));
			rs.close();
			stmt.close();
		}
	}

	public static void adminMenu(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String answer = null;

		while(true){
			System.out.println();
			System.out.println("-----Operations for administrator menu-----");
			System.out.println("What kinds of operation would you like to perform?");
			System.out.println("1. Create all tables");
			System.out.println("2. Delete all tables");
			System.out.println("3. Load from a dataset");
			System.out.println("4. Show number of records in each table");
			System.out.println("0. Return to the main menu");
			System.out.print("Enter Your Choice: ");
			answer = menuAns.nextLine();

			if(answer.equals("1")||answer.equals("2")||answer.equals("3")||answer.equals("4")||answer.equals("0"))
				break;
			System.out.println("[Error]: Wrong Input, Type in again!!!");
		}

		if(answer.equals("1")){
			createTables(mySQLDB);
		}else if(answer.equals("2")){
			deleteTables(mySQLDB);
		}else if(answer.equals("3")){
			loadTables(menuAns, mySQLDB);
		}else if(answer.equals("4")){
			showTables(menuAns, mySQLDB);
		}
	}

	public static void searchNEAs(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String ans = null, keyword = null, method = null, ordering = null;
		String searchSQL = "";
		PreparedStatement stmt = null;

		searchSQL += "SELECT N.NID, N.Distance, N.Family, N.Duration, N.Energy, C.RType ";
		searchSQL += "FROM NEA N, Contain C ";
		searchSQL += "WHERE N.NID = C.NID";

		while(true){
			System.out.println("Choose the Search criterion:");
			System.out.println("1. ID");
			System.out.println("2. Family");
            System.out.println("3. Resource Type");
			System.out.print("My criterion: ");
			ans = menuAns.nextLine();
			if(ans.equals("1")||ans.equals("2")||ans.equals("3")) break;
		}
		method = ans;

		while(true){
			System.out.print("Type in the Search Keyword: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}
		keyword = ans;

		/*while(true){
			System.out.println("Choose ordering:");                                  
			System.out.println("1. By price, ascending order");
			System.out.println("2. By price, descending order");              
			System.out.print("Choose the search criterion: ");
			ans = menuAns.nextLine();
			if(ans.equals("1")||ans.equals("2")) break;
		}   
		ordering = ans;*/

		if(method.equals("1")){
			searchSQL += " AND N.NID = ? ";
		}else if(method.equals("2")){
			searchSQL += " AND N.Family LIKE ? ";
		}else if(method.equals("3")){
			searchSQL += " AND C.RType LIKE ? ";
		}

		/*if(ordering.equals("1")){
			searchSQL += " ORDER BY P.p_price ASC";
		}else if(ordering.equals("2")){
			searchSQL += " ORDER BY P.p_price DESC";
		}*/

		stmt = mySQLDB.prepareStatement(searchSQL);
		stmt.setString(1, "%" + keyword + "%");
        
        if(method.equals("1")){
			stmt.setString(1, keyword);
		}else if(method.equals("2")){
			stmt.setString(1, "%" + keyword + "%");
		}else if(method.equals("3")){
			stmt.setString(1, "%" + keyword + "%");
		}

		String[] field_name = {"ID", "Distance", "Family", "Duration", "Energy", "Resources"};
		for (int i = 0; i < 6; i++){
			 System.out.print("| " + field_name[i] + " ");
		}
		System.out.println("|");

		ResultSet resultSet = stmt.executeQuery();
		while(resultSet.next()){
			for (int i = 1; i <= 6; i++){
				System.out.print("| " + resultSet.getString(i) + " ");
			}    
			System.out.println("|");
		}
		System.out.println("End of Query");
		resultSet.close();
		stmt.close();
	}
    
    public static void searchSpacecrafts(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String ans = null, keyword = null, method = null, ordering = null;
		String searchSQL = "";
		PreparedStatement stmt = null;

		searchSQL += "SELECT * FROM (SELECT S.Agency, S.MID, S.Num, IF(A.Capacity IS NULL, 'E', 'A') AS Type, S.Energy, S.Duration, A.Capacity, S.Charge ";
		searchSQL += "FROM SpacecraftModel S ";
		searchSQL += "LEFT JOIN A_Model A ";
        searchSQL += "ON S.Agency = A.Agency AND S.MID = A.MID) AS TEMP WHERE";

		while(true){
			System.out.println("Choose the Search criterion:");
			System.out.println("1. Agency Name");
			System.out.println("2. Type");
            System.out.println("3. Least energy [km/s]");
            System.out.println("4. Least working time [days]");
            System.out.println("5. Least capacity [m^3]");
			System.out.print("My criterion: ");
			ans = menuAns.nextLine();
			if(ans.equals("1")||ans.equals("2")||ans.equals("3")||ans.equals("4")||ans.equals("5")) break;
		}
		method = ans;

		while(true){
			System.out.print("Type in the Search Keyword: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}
		keyword = ans;

		if(method.equals("1")){
			searchSQL += " Agency = ? ";
		}else if(method.equals("2")){
			searchSQL += " Type = ? ";
		}else if(method.equals("3")){
			searchSQL += " Energy > ? ";
		}else if(method.equals("4")){
			searchSQL += " Duration > ? ";
		}else if(method.equals("5")){
			searchSQL += " Capacity > ? ";
		}

		stmt = mySQLDB.prepareStatement(searchSQL);
        if(method.equals("1")){
			stmt.setString(1, keyword);
		}else if(method.equals("2")){
			stmt.setString(1, keyword);
		}else if(method.equals("3")){
			stmt.setDouble(1, Double.parseDouble(keyword));
		}else if(method.equals("4")){
			stmt.setInt(1, Integer.parseInt(keyword));
		}else if(method.equals("5")){
			stmt.setInt(1, Integer.parseInt(keyword));
		}

		String[] field_name = {"Agency", "MID", "SNum", "Type", "Energy", "T", "Capacity", "Charge"};
		for (int i = 0; i < 8; i++){
			 System.out.print("| " + field_name[i] + " ");
		}
		System.out.println("|");

		ResultSet resultSet = stmt.executeQuery();
		while(resultSet.next()){
			for (int i = 1; i <= 8; i++){
				System.out.print("| " + resultSet.getString(i) + " ");
			}    
			System.out.println("|");
		}
		System.out.println("End of Query");
		resultSet.close();
		stmt.close();
	}
    
    public static void NEAExploration(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String ans = null, keyword = null, ordering = null;
		String searchSQL = "";
		PreparedStatement stmt = null;
        
        Statement prestmt  = mySQLDB.createStatement();
		prestmt.execute("CREATE OR REPLACE VIEW T1 AS SELECT A.Agency, A.MID, A.Num, R.SNum, R.CheckoutDate, R.ReturnDate, A.Charge, A.Duration, A.Energy, A.Capacity FROM RentalRecord R, A_Model A WHERE (R.ReturnDate IS NOT NULL OR (R.CheckoutDate IS NULL AND R.ReturnDate IS NULL)) AND R.Agency=A.Agency AND R.MID=A.MID");
		prestmt.execute("CREATE OR REPLACE VIEW T2 AS SELECT N.NID, N.Distance, N.Family, N.Duration, N.Energy, C.Rtype FROM NEA N, Contain C WHERE N.NID=C.NID");
		prestmt.execute("CREATE OR REPLACE VIEW T3 AS SELECT T2.*, R.Density, R.Value FROM T2 LEFT JOIN Resource R ON T2.RType=R.RType");
        
        searchSQL = "SELECT T1.Agency, T1.MID, T1.SNum ,(T1.Charge * T3.Duration)AS Cost, ((IF(T3.Value IS NULL,0,T3.Value)*IF(T3.Density IS NULL,0,T3.Density)*T1.Capacity)-(T1.Charge * T3.Duration))AS Benefit FROM T1, T3 WHERE T1.Energy>T3.Energy AND T1.Duration>T3.Duration AND T3.NID=?";

		while(true){
			System.out.print("Type in the Search Keyword: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}
		keyword = ans;
        
        searchSQL += " ORDER BY Benefit DESC";

		stmt = mySQLDB.prepareStatement(searchSQL);
        stmt.setString(1, keyword);
        
        System.out.println("All possible solutions:");

		String[] field_name = {"Agency", "MID", "SNum", "Cost", "Benefit"};
		for (int i = 0; i < 5; i++){
			 System.out.print("| " + field_name[i] + " ");
		}
		System.out.println("|");

		ResultSet resultSet = stmt.executeQuery();
		while(resultSet.next()){
			for (int i = 1; i <= 5; i++){
				System.out.print("| " + resultSet.getString(i) + " ");
			}    
			System.out.println("|");
		}
		System.out.println("End of Query");
		resultSet.close();
		stmt.close();
        prestmt.execute("DROP VIEW T1");
        prestmt.execute("DROP VIEW T2");
        prestmt.execute("DROP VIEW T3");
        prestmt.close();
	}
    
    public static void BeneficialExploration(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String ans = null, budget = null, rtype = null, ordering = null;
		String searchSQL = "";
		PreparedStatement stmt = null;
        
        Statement prestmt  = mySQLDB.createStatement();
		prestmt.execute("CREATE OR REPLACE VIEW T1 AS SELECT A.Agency, A.MID, A.Num, R.SNum, R.CheckoutDate, R.ReturnDate, A.Charge, A.Duration, A.Energy, A.Capacity FROM RentalRecord R, A_Model A WHERE (R.ReturnDate IS NOT NULL OR (R.CheckoutDate IS NULL AND R.ReturnDate IS NULL)) AND R.Agency=A.Agency AND R.MID=A.MID");
		prestmt.execute("CREATE OR REPLACE VIEW T2 AS SELECT N.NID, N.Distance, N.Family, N.Duration, N.Energy, C.Rtype FROM NEA N, Contain C WHERE N.NID=C.NID");
		prestmt.execute("CREATE OR REPLACE VIEW T3 AS SELECT T2.*, R.Density, R.Value FROM T2 LEFT JOIN Resource R ON T2.RType=R.RType");
        prestmt.execute("CREATE OR REPLACE VIEW T4 AS SELECT T3.NID, T3.Family, T1.Agency, T1.MID, T1.SNum , T3.Duration,(T1.Charge * T3.Duration)AS Cost, ((IF(T3.Value IS NULL,0,T3.Value)*IF(T3.Density IS NULL,0,T3.Density)*T1.Capacity)-(T1.Charge * T3.Duration))AS Benefit, T3.RType FROM T1, T3 WHERE T1.Energy>T3.Energy AND T1.Duration>T3.Duration");
        
        searchSQL = "SELECT * FROM T4,(SELECT MAX(Benefit)AS MAXBenefit FROM T4 WHERE Cost<=? AND RType=?)AS T5 WHERE T4.Benefit=T5.MAXBenefit AND T4.Cost<=? AND T4.RType=?";

		while(true){
			System.out.print("Type in the your budget [$]: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}
		budget = ans;
        
        while(true){
			System.out.print("Type in the resource type: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}
        rtype = ans;

		stmt = mySQLDB.prepareStatement(searchSQL);
        stmt.setString(1, budget);
        stmt.setString(2, rtype);
        stmt.setString(3, budget);
        stmt.setString(4, rtype);
        
        System.out.println("The most beneficial mission is:");

		String[] field_name = {"NEA ID", "Family", "Agency", "MID", "SNum", "Duration", "Cost", "Benefit"};
		for (int i = 0; i < 8; i++){
			 System.out.print("| " + field_name[i] + " ");
		}
		System.out.println("|");

		ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        for (int i = 1; i <= 8; i++){
            System.out.print("| " + resultSet.getString(i) + " ");
        }    
        System.out.println("|");
		System.out.println("End of Query");
		resultSet.close();
		stmt.close();
        prestmt.execute("DROP VIEW T1");
        prestmt.execute("DROP VIEW T2");
        prestmt.execute("DROP VIEW T3");
        prestmt.execute("DROP VIEW T4");
        prestmt.close();
	}

	public static void rentSpacecraft(Scanner menuAns, Connection mySQLDB) throws SQLException{
        String selectSQL = "SELECT COUNT(*) FROM RentalRecord R WHERE R.Agency=? AND R.MID=? AND R.SNum=? AND (R.ReturnDate IS NOT NULL OR (R.CheckoutDate IS NULL AND R.ReturnDate IS NULL))";
		String updateSQL = "UPDATE RentalRecord R set R.CheckoutDate = ?, R.ReturnDate = NULL WHERE R.Agency=? AND R.MID=? AND R.SNum=?";
        Calendar calendar = Calendar.getInstance();
        java.sql.Date DateObject = new java.sql.Date(calendar.getTime().getTime());

		String Agency = null, MID = null, SNum = null;

		while(true){
			System.out.print("Enter the space agency name: ");
			Agency = menuAns.nextLine();
			if(!Agency.isEmpty()) break;
		}

		while(true){
			System.out.print("Enter the MID: ");
			MID = menuAns.nextLine();
			if(!MID.isEmpty()) break;
		}
        
        while(true){
			System.out.print("Enter the SNum: ");
			SNum = menuAns.nextLine();
			if(!SNum.isEmpty()) break;
		}

		PreparedStatement stmt = mySQLDB.prepareStatement(selectSQL);
		stmt.setString(1, Agency);
        stmt.setString(2, MID);
        stmt.setString(3, SNum);
		
		ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        /*System.out.print("| " + resultSet.getInt(1) + " |");
        System.out.println();
        System.out.println("End of Query");*/
        if(resultSet.getInt(1)!=1){
            System.err.println("[Error]: This spacecraft is not available to be rented");
            resultSet.close();
            stmt.close();
            return;
        }
		
        resultSet.close();
        //System.out.println("hello");
        
		/*if(retVal == 0){
			System.err.println("[Error]: This Product is currently out of stock");
			return;
		}*/
		stmt.close();
        
        System.out.println(DateObject);

		PreparedStatement stmt2 = mySQLDB.prepareStatement(updateSQL);
		stmt2.setDate(1, DateObject);
		stmt2.setString(2, Agency);
        stmt2.setString(3, MID);
        stmt2.setString(4, SNum);
		stmt2.executeUpdate();
		stmt2.close();

		/*PreparedStatement stmt3 = mySQLDB.prepareStatement(remainQuantitySQL);
		stmt3.setString(1, p_id);  
		ResultSet resultSet = stmt3.executeQuery();
		resultSet.next();
		System.out.println("Product: "+ resultSet.getString(2) + "(id: " + resultSet.getString(1) + ") Remaining Quality: " + resultSet.getString(3));

		stmt3.close();*/
	}

	public static void customersMenu(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String answer = "";

		while(true){
			System.out.println();
			System.out.println("-----Operations for explorational companies (rental customers)-----");
			System.out.println("What kinds of operation would you like to perform?");
			System.out.println("1. Search for NEAs based on some criteria");
			System.out.println("2. Search for spacecrafts based on some criteria");
            System.out.println("3. A certain NEA exploration mission design");
            System.out.println("4. The most beneficial NEA exploration mission design");
			System.out.println("0. Return to the main menu");
			System.out.print("Enter Your Choice: ");
			answer = menuAns.nextLine();
			
			if(answer.equals("1")||answer.equals("2")||answer.equals("3")||answer.equals("4")||answer.equals("0"))
				break;
			System.out.println("[Error]: Wrong Input, Type in again!!!");
		}
		
		if(answer.equals("1")){
			searchNEAs(menuAns, mySQLDB);
		}else if(answer.equals("2")){
			searchSpacecrafts(menuAns, mySQLDB);
		}else if(answer.equals("3")){
			NEAExploration(menuAns, mySQLDB);
		}else if(answer.equals("4")){
			BeneficialExploration(menuAns, mySQLDB);
		}
	}

	public static void countSalespersonRecord(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String recordSQL = "SELECT S.s_id, S.s_name, S.s_experience, COUNT(T.t_id) ";
		recordSQL += "FROM transaction T, salesperson S ";
		recordSQL += "WHERE T.s_id = S.s_id AND S.s_experience >= ? AND S.s_experience <= ? ";
		recordSQL += "GROUP BY S.s_id, S.s_name, S.s_experience ";
		recordSQL += "ORDER BY S.s_id DESC";
		
		String expBegin = null, expEnd = null;

		while(true){
			System.out.print("Type in the lower bound for years of experience: ");
			expBegin = menuAns.nextLine();
			if(!expBegin.isEmpty()) break;
		}

		while(true){
			System.out.print("Type in the upper bound for years of experience: ");
			expEnd = menuAns.nextLine();
			if(!expEnd.isEmpty()) break;
		}

		PreparedStatement stmt  = mySQLDB.prepareStatement(recordSQL);
		stmt.setInt(1, Integer.parseInt(expBegin));
		stmt.setInt(2, Integer.parseInt(expEnd));
		
		ResultSet resultSet = stmt.executeQuery();

		System.out.println("Transaction Record:");
		
		System.out.println("| ID | Name | Years of Experience | Number of Transaction |");
		while(resultSet.next()){
			for (int i = 1; i <= 4; i++){
				System.out.print("| " + resultSet.getString(i) + " ");
			}
			System.out.println("|");
		}
		System.out.println("End of Query");
	}

	public static void showPopularPart(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String ans;
		int booknum = 0, i = 0;
		String sql = "SELECT P.p_id, P.p_name, count(*) "+
					 "FROM part P, transaction T "+
					 "WHERE P.p_id = T.p_id "+
					 "GROUP BY P.p_id, P.p_name "+
					 "ORDER BY count(*) DESC";

		while(true){
			System.out.print("Type in the number of parts: ");
			ans = menuAns.nextLine();
			if(!ans.isEmpty()) break;
		}

		booknum = Integer.parseInt(ans);
		Statement stmt  = mySQLDB.createStatement();
		ResultSet resultSet = stmt.executeQuery(sql);
		System.out.println("| Part ID | Part Name | No. of Transaction |");
		while(resultSet.next() && i < booknum){
			System.out.println( "| " + resultSet.getString(1) + " " +
								"| " + resultSet.getString(2) + " " +
								"| " + resultSet.getString(3) + " " +
								"|");
			i++;
		}
		System.out.println("End of Query");
		stmt.close();
	}

	public static void showTotalSales(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String sql = "SELECT M.m_id, M.m_name, SUM(P.p_price) as total_sum "+
					 "FROM transaction T, part P, manufacturer M " +
					 "WHERE T.p_id = P.p_id AND P.m_id = M.m_id " +
					 "GROUP BY M.m_id, M.m_name " + 
					 "ORDER by total_sum DESC";

		Statement stmt  = mySQLDB.createStatement();
		ResultSet resultSet = stmt.executeQuery(sql);
	
		System.out.println("| Manufacturer ID | Manufacturer Name | Total Sales Value |");
		while(resultSet.next()){
			System.out.println(	"| " + resultSet.getString(1) + " " +
								"| " + resultSet.getString(2) + " " +          
								"| " + resultSet.getString(3) + " " + 
								"|"); 
		}
		System.out.println("End of Query");
		stmt.close();
	}

	public static void staffMenu(Scanner menuAns, Connection mySQLDB) throws SQLException{
		String answer = "";

		while(true){
			System.out.println();
			System.out.println("-----Operations for spacecraft rental staff-----");
			System.out.println("What kinds of operation would you like to perform?");
			System.out.println("1. Rent a spacecraft");
			System.out.println("2. Return a spacecraft");
			System.out.println("3. List all spacecrafts currently rented out (on a mission) for a certain period");
			System.out.println("4. List the number of spacecrafts currently rented out by each agency");
            System.out.println("0. Return to the main menu");
			System.out.print("Enter Your Choice: ");
			answer = menuAns.nextLine();

			if(answer.equals("1")||answer.equals("2")||answer.equals("3")||answer.equals("4")||answer.equals("0"))
				break;
			System.out.println("[Error]: Wrong Input, Type in again!!!");
		}

		if(answer.equals("1")){
			rentSpacecraft(menuAns, mySQLDB);
		}else if(answer.equals("2")){
			showTotalSales(menuAns, mySQLDB);
		}else if(answer.equals("3")){
			showPopularPart(menuAns, mySQLDB);
		}else if(answer.equals("4")){
			showPopularPart(menuAns, mySQLDB);
		}
	}

	public static void main(String[] args) {
		Scanner menuAns = new Scanner(System.in);
		System.out.println("Welcome to mission design system!");

		while(true){
			try{
				Connection mySQLDB = connectToOracle();
				System.out.println();
				System.out.println("-----Main menu-----");
				System.out.println("What kinds of operation would you like to perform?");
				System.out.println("1. Operations for administrator");
				System.out.println("2. Operations for explorational companies (rental customers)");
				System.out.println("3. Operations for spacecraft rental staff");
				System.out.println("0. Exit this program");
				System.out.print("Enter Your Choice: ");

				String answer = menuAns.nextLine();

				if(answer.equals("1")){
					adminMenu(menuAns, mySQLDB);
				}else if(answer.equals("2")){
					customersMenu(menuAns, mySQLDB);
				}else if(answer.equals("3")){
					staffMenu(menuAns, mySQLDB);
				}else if(answer.equals("0")){
					break;
				}else{
					System.out.println("[Error]: Wrong Input, Type in again!!!");
				}
			}catch (SQLException e){
				System.out.println(e);
			}
		}

		menuAns.close();
		System.exit(0);
	}
}