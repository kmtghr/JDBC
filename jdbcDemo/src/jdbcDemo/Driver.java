package jdbcDemo;

import java.sql.*;
public class Driver {
	
	public static void printEmployees (ResultSet rs) throws Exception {
		System.out.println("Emp_ID\tFirst_Name\tLast_Name\tBirth_Date\tSex\tSalary\tSuper_ID\tBranch_ID" );
		/*int super_Id = rs.getInt(7);
		if( rs.wasNull())	super_Id = 0;
		int branch_Id = rs.getInt(8);
		if( rs.wasNull())	branch_Id = 0;*/
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4) + "\t" + rs.getString(5) + "\t" + 
					rs.getInt(6) + "\t" + rs.getInt(7) + "\t" + rs.getInt(8));
		}
	}
	
	public static void printBranches (ResultSet rs) throws Exception {
		System.out.println("Branch_ID\tBranch_Name\tMgr_ID\tMgr_Join_Date" );
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3) + "\t" + rs.getString(4));
		}
	}
	
	public static void printSuppliers (ResultSet rs) throws Exception {
		System.out.println("Branch_ID\tSupplier_Name\tSupply_Type" );
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
		}
	}
	
	public static void printClients (ResultSet rs) throws Exception {
		System.out.println("Branch_ID\tSupplier_Name\tSupply_Type" );
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3));
		}
	}
	
	public static void printWorksWith(ResultSet rs) throws Exception {
		System.out.println("Emp_ID\tClient_ID\tTot_Sales" );
		while(rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getInt(2) + "\t" + rs.getInt(3));
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection myConn = null;
		Statement myStmt = null;
		PreparedStatement pStmt = null;
		try {
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/company", "root", "RootPassword");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			//----------------------------------------------------------------------------------------------------------->
			// Create employee table
			Boolean b = myStmt.execute("CREATE TABLE employee (\r\n"
					+ "  emp_id INT PRIMARY KEY,\r\n"
					+ "  first_name VARCHAR(40),\r\n"
					+ "  last_name VARCHAR(40),\r\n"
					+ "  birth_day DATE,\r\n"
					+ "  sex VARCHAR(1),\r\n"
					+ "  salary INT,\r\n"
					+ "  super_id INT,\r\n"
					+ "  branch_id INT\r\n"
					+ ");");
			
			System.out.println("Employee table CREATEd successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Create branch table
			b = myStmt.execute("CREATE TABLE branch (\r\n"
					+ "  branch_id INT PRIMARY KEY,\r\n"
					+ "  branch_name VARCHAR(40),\r\n"
					+ "  mgr_id INT,\r\n"
					+ "  mgr_start_date DATE,\r\n"
					+ "  FOREIGN KEY(mgr_id) REFERENCES employee(emp_id) ON DELETE SET NULL\r\n"
					+ ");");
			
			System.out.println("Branch table CREATEd successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Add foreign key super_id to employee table
			b = myStmt.execute("ALTER TABLE employee ADD FOREIGN KEY(super_id) REFERENCES employee(emp_id) ON DELETE SET NULL;");
			
			System.out.println("Employee table ALTERed successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Add foreign key dept_id to employee table
			b = myStmt.execute("ALTER TABLE employee ADD FOREIGN KEY(branch_id) REFERENCES branch(branch_id) ON DELETE SET NULL;");
			
			System.out.println("Employee table ALTERed successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Create client table
			b = myStmt.execute("CREATE TABLE client (\r\n"
					+ "  client_id INT PRIMARY KEY,\r\n"
					+ "  client_name VARCHAR(40),\r\n"
					+ "  branch_id INT,\r\n"
					+ "  FOREIGN KEY(branch_id) REFERENCES branch(branch_id) ON DELETE SET NULL\r\n"
					+ ");");
			
			System.out.println("Client table CREATEd successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Create works_with table
			b = myStmt.execute("CREATE TABLE works_with (\r\n"
					+ "  emp_id INT,\r\n"
					+ "  client_id INT,\r\n"
					+ "  total_sales INT,\r\n"
					+ "  PRIMARY KEY(emp_id, client_id),\r\n"
					+ "  FOREIGN KEY(emp_id) REFERENCES employee(emp_id) ON DELETE CASCADE,\r\n"
					+ "  FOREIGN KEY(client_id) REFERENCES client(client_id) ON DELETE CASCADE\r\n"
					+ ");");
			
			System.out.println("Works_with table CREATEd successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			// Create supplier table
			b = myStmt.execute("CREATE TABLE branch_supplier (\r\n"
					+ "  branch_id INT,\r\n"
					+ "  supplier_name VARCHAR(40),\r\n"
					+ "  supply_type VARCHAR(40),\r\n"
					+ "  PRIMARY KEY(branch_id, supplier_name),\r\n"
					+ "  FOREIGN KEY(branch_id) REFERENCES branch(branch_id) ON DELETE CASCADE\r\n"
					+ ");");
			
			System.out.println("Supplier table CREATEd successfully.");
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			int n = myStmt.executeUpdate("INSERT INTO employee VALUES(100, 'Dravid', 'Joshi', '1967-11-17', 'M', 2500000, NULL, NULL);");
			System.out.println(n + " row(s) updated.");
			ResultSet myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO branch VALUES(1, 'Chennai', 100, '2006-02-09');");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM branch");
			printBranches(myRs);

			n = myStmt.executeUpdate("UPDATE employee\r\n"
					+ "SET branch_id = 1\r\n"
					+ "WHERE emp_id = 100;");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO employee VALUES \r\n"
					+ "(101, 'Rakul', 'Mishra', '1961-05-11', 'F', 1100000, 100, 1);");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			n = myStmt.executeUpdate("INSERT INTO employee VALUES(102, 'Manish', 'Singh', '1964-03-15', 'M', 2300000, 100, NULL);");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO branch VALUES(2, 'Mumbai', 102, '1992-04-06');");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM branch");
			printBranches(myRs);

			n = myStmt.executeUpdate("UPDATE employee\r\n"
					+ "SET branch_id = 2\r\n"
					+ "WHERE emp_id = 102;");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO employee VALUES \r\n"
					+ "(103, 'Archana', 'Mehta', '1971-06-25', 'F', 1200000, 102, 2),\r\n"
					+ "(104, 'Kavita', 'Kapoor', '1980-02-05', 'F', 1060000, 102, 2),\r\n"
					+ "(105, 'Santosh', 'Heth', '1958-02-19', 'M', 1140000, 102, 2);");
			myRs = myStmt.executeQuery("SELECT * FROM employee;");
			printEmployees(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			n = myStmt.executeUpdate("INSERT INTO employee VALUES(106, 'Jayesh', 'Patel', '1969-09-05', 'M', 780000, 100, NULL);");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO branch VALUES(3, 'Indore', 106, '1998-02-13');");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM branch");
			printBranches(myRs);

			n = myStmt.executeUpdate("UPDATE employee\r\n"
					+ "SET branch_id = 3\r\n"
					+ "WHERE emp_id = 106;");
			System.out.println(n + " row(s) updated.");
			myRs = myStmt.executeQuery("SELECT * FROM employee");
			printEmployees(myRs);
			
			n = myStmt.executeUpdate("INSERT INTO employee VALUES \r\n"
					+ "(107, 'Amit', 'Singhal', '1973-07-22', 'M', 650000, 106, 3),\r\n"
					+ "(108, 'Jignesh', 'Heth', '1978-10-01', 'M', 71000, 106, 3);");
			myRs = myStmt.executeQuery("SELECT * FROM employee;");
			printEmployees(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			n = myStmt.executeUpdate("INSERT INTO branch_supplier VALUES\r\n"
					+ "(2, 'Hammer Mill', 'Paper'), \r\n"
					+ "(3, 'Patriot Paper', 'Paper'), \r\n"
					+ "(2, 'J.T. Forms & Labels', 'Custom Forms'), \r\n"
					+ "(2, 'Uni-ball', 'Writing Utensils'), \r\n"
					+ "(3, 'Uni-ball', 'Writing Utensils'), \r\n"
					+ "(3, 'Hammer Mill', 'Paper'), \r\n"
					+ "(3, 'Stamford Lables', 'Custom Forms');");
			System.out.println(n + " row(s) updated.");
			
			myRs = myStmt.executeQuery("SELECT * FROM branch_supplier;");
			printSuppliers(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			n = myStmt.executeUpdate("INSERT INTO client VALUES \r\n"
					+ "(400, 'Dunmore Highschool', 2), \r\n"
					+ "(401, 'Lackawana Country', 2), \r\n"
					+ "(402, 'FedEx', 3), \r\n"
					+ "(403, 'John Daly Law, LLC', 3);");
			System.out.println(n + " row(s) updated.");
			
			myRs = myStmt.executeQuery("SELECT * FROM client;");
			printClients(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			n = myStmt.executeUpdate("INSERT INTO works_with VALUES \r\n"
					+ "(105, 400, 55000), \r\n"
					+ "(102, 401, 267000), \r\n"
					+ "(108, 402, 22500), \r\n"
					+ "(107, 403, 5000);");
			System.out.println(n + " row(s) updated.");
			
			myRs = myStmt.executeQuery("SELECT * FROM works_with;");
			printWorksWith(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
			
			//----------------------------------------------------------------------------------------------------------->
			String sql = "UPDATE works_with \r\n"
					+ "SET total_sales = ? \r\n"
					+ "WHERE emp_id = ? AND client_id = ?;";
			pStmt = myConn.prepareStatement(sql);
			pStmt.setInt(1, 156000);
			pStmt.setInt(2, 102);
			pStmt.setInt(3, 401);
			
			myRs = myStmt.executeQuery("SELECT * FROM works_with WHERE emp_id = 102 AND client_id = 401;");
			printWorksWith(myRs);
			int rows = pStmt.executeUpdate();
			System.out.println("Rows Impacted:" + rows);
			
			sql = "SELECT * FROM works_with WHERE emp_id = ? AND client_id = ?;";
			pStmt = myConn.prepareStatement(sql);
			pStmt.setInt(1, 102);
			pStmt.setInt(2, 401);
			myRs = pStmt.executeQuery();
			printWorksWith(myRs);
			//----------------------------------------------------------------------------------------------------------->
			
			
		} 
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			try{
		       if(myStmt!=null)
		          myStmt.close();
		    }catch(SQLException se2){
		    }// nothing we can do
			try{
			   if(pStmt!=null)
			      pStmt.close();
			}catch(SQLException se2){
			}
		    try{
		       if(myConn!=null)
		          myConn.close();
		    }catch(SQLException se){
		       se.printStackTrace();
		    }
		}
	}

}
