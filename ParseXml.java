import java.lang.String;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

// Question Analysis

// To deal with large data in XML format, StAX is the best way to read file
// If we want to do some join between different file, we need to store the parsed data.
// As the data is very large, my thought is to use database. 
// But if we have a huge memory and don't need to worry about heap limitation, the hashMap will be a good choice. ("memory mode")
// I am using SQLite, which is embedded into the program and is easy to setup.
// Since using DB, we don't need class for user and poster anymore.
// I still remained the User and Post class, in order to quick change to "memory mode" when we have the ability.
// Also, we don't need to maintain the arrays to store the result.


// Database Table

// QUESTION TABLE
// ID INT PRIMARY KEY NOT NULL
// POSTTYPEID INT NOT NULL
// OWNERUSERID INT NOT NULL
//
// ANSWER TABLE
// ID INT PRIMARY KEY NOT NULL
// POSTTYPEID INT NOT NULL
// OWNERUSERID INT NOT NULL
//
// USER TABLE
// ID PRIMARY KEY NOT NULL
// USERNAME VCHAR NOT NULL



// Future Improvement

// 1.
// Make the table more subtle
// For example: we can split question/answer table into sub tables by owneruserid
// Owneruserid start with the same digit go to one table.
// Then when we select from the question/answer table, the table size will be smaller.

// 2.
// Rewrite the sql to make it more effective.

// 2.
// Redesign database table, make it more effective
// For example1: for question and answer table, we can use owneruserid as primary key
// Precalculate the user's post numbers and join with user table to get the username
// (Like the HashMap design in "memory mode")

// 3.
// Change to more effective database, like mySql or even Oracle, SQL server

// 4.
// Use Hadoop or similar implementations to design MapReduce programming mode
// Make the program run parallel and distributed on a cluster
// Use HBase or similar distribute database to store data

public class ParseXml {

	private ArrayList<MapData> topQuestion = new ArrayList<MapData>();
	private ArrayList<MapData> topAnswer = new ArrayList<MapData>();

    public ParseXml() {
		//users = new ArrayList<User>();
		//posts = new ArrayList<Post>();
	}

    // Represents entries in users.xml
    // Some fields omitted due to laziness
	class User {
		public String Id;
		public String DisplayName;
		public HashMap<String, String> userMap = new HashMap<String, String>();

		public void addUser(String id, String displayName) {
			if (id == null || displayName == null)
				return;
			if (userMap.containsKey(id)) {
				userMap.put(id, displayName);
			} else {
				return; // if user is duplicate, will be ignored.
			}
		}
	};

    // Represents entries in posts.xml
    // Some fields omitted due to laziness
	class Post {
		public String Id;
		public String PostTypeId;
		public String OwnerUserId;
		public HashMap<String, Integer> postQuestionMap = new HashMap<String, Integer>();
		public HashMap<String, Integer> postAnswerMap = new HashMap<String, Integer>();
		public void addPost(String id, String postTypeId, String ownerId) {
			if (id == null || postTypeId == null || ownerId == null)
				return;
			// add question
			if (postTypeId == "1") {
				if (postQuestionMap.containsKey(ownerId)) {
					postQuestionMap.put(ownerId,
							postQuestionMap.get(ownerId) + 1);
				} else {
					postQuestionMap.put(ownerId, 1);
				}
			}
			// add answer
			else if (postTypeId == "2") {
				if (postAnswerMap.containsKey(ownerId)) {
					postAnswerMap.put(ownerId, postAnswerMap.get(ownerId) + 1);
				} else {
					postAnswerMap.put(ownerId, 1);
				}
			} else {
				return;
			}
		}
	};

	// Some data for the map
	class MapData {
		public String DisplayName;
		public int Count;
	};

	public ArrayList<MapData> getTopQuestion(){
		return this.topQuestion;
	}

	public ArrayList<MapData> getTopAnswer(){
		return this.topAnswer;
	}


	public void parsePost(String fileName) {
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		Connection c = null;
		Statement stmt = null;
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new FileInputStream(fileName));
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:test.db");
			c.setAutoCommit(false);
			stmt = c.createStatement();
			int event;
			int i = 0;
			while (xmlStreamReader.hasNext()) {
				i++;
				event = xmlStreamReader.next();
				if (event == XMLStreamConstants.START_ELEMENT)
					if (xmlStreamReader.getLocalName().equals("row")) {
						try {
							// xmlStreamReader.getAttributeValue if not found,
							// will return null.
							int id = Integer.parseInt(xmlStreamReader
									.getAttributeValue(null, "Id"));
							int postTypeId = Integer.parseInt(xmlStreamReader
									.getAttributeValue(null, "PostTypeId"));
							int ownerUserId = Integer.parseInt(xmlStreamReader
									.getAttributeValue(null, "OwnerUserId"));

							if (postTypeId == 1) { // Question entry
								String sql = "INSERT INTO QUESTION (ID,POSTTYPEID,OWNERUSERID) "
										+ "VALUES ("
										+ id
										+ ","
										+ postTypeId
										+ "," + ownerUserId + ");";
								stmt.executeUpdate(sql);
							} else if (postTypeId == 2) { // Answer Entry
								String sql = "INSERT INTO ANSWER (ID,POSTTYPEID,OWNERUSERID) "
										+ "VALUES ("
										+ id
										+ ","
										+ postTypeId
										+ "," + ownerUserId + ");";
								stmt.executeUpdate(sql);
							}
						} catch (Exception e) {
							System.out.println("invalid post entry. "
									+ e.getClass().getName() + ": "
									+ e.getMessage());
							continue;
						}
					}
				// Every 100 entries commit once
				if (i == 100) {
					i = 0;
					c.commit();
				}
			}
			c.commit();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.out.println("Failed to parse post. "
					+ e.getClass().getName() + ": " + e.getMessage());
		}
	}


	public void parseUser(String fileName) {
		Connection c = null;
		Statement stmt = null;
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new FileInputStream(fileName));
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:test.db");
			c.setAutoCommit(false);
			stmt = c.createStatement();
			int event;
			int i = 0;
			while (xmlStreamReader.hasNext()) {
				event = xmlStreamReader.next();
				i++;
				if (event == XMLStreamConstants.START_ELEMENT)
					if (xmlStreamReader.getLocalName().equals("row")) {
						try {
							// xmlStreamReader.getAttributeValue if not found,
							// will return null.
							int id = Integer.parseInt(xmlStreamReader
									.getAttributeValue(null, "Id"));
							String displayName = xmlStreamReader
									.getAttributeValue(null, "DisplayName");
							if (displayName == null) {
								throw new Exception("Empty DisplayName");
							}
							String sql = "INSERT INTO USER (ID,USERNAME) "
									+ "VALUES (" + id + ",'" + displayName
									+ "');";
							stmt.executeUpdate(sql);
						} catch (Exception e) {
							System.out.println("invalid user entry. "
									+ e.getClass().getName() + ": "
									+ e.getMessage());
							continue;
						}
					}
				// Every 100 entries commit once
				if (i == 100) {
					i = 0;
					c.commit();
				}
			}
			c.commit();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.out.println("Failed to parse. " + e.getClass().getName()
					+ ": " + e.getMessage());
		}
	}

	public void printUserCount(int postTypeId) {
		try {
			Class.forName("org.sqlite.JDBC");
			Connection c = DriverManager.getConnection("jdbc:sqlite:test.db");
			Statement stmt = c.createStatement();
			Statement stmt2 = c.createStatement();
			ResultSet rs;
			String sql;
			ArrayList<MapData> tmplist;
			if (postTypeId == 1){
				sql = "SELECT COUNT(id),OWNERUSERID FROM QUESTION GROUP BY owneruserid ORDER BY COUNT(id) DESC LIMIT 10";
				tmplist = topQuestion;
			}
			else if(postTypeId == 2){
				sql = "SELECT COUNT(id),OWNERUSERID FROM ANSWER GROUP BY owneruserid ORDER BY COUNT(id) DESC LIMIT 10";
				tmplist = topAnswer;
			}
			else
				throw new Exception("Invalid Post Type Id");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int countnums = rs.getInt(1);
				int userid = rs.getInt(2);
				sql = "SELECT USERNAME FROM USER WHERE ID =="+Integer.toString(userid);
				ResultSet tmp = stmt2.executeQuery(sql);
				String username = tmp.getString("username");				
				System.out.println("UserName: " + username + ", PostNum: "
						+ countnums + ".");

				// Store result to arraylist
				MapData md = new MapData();
				md.DisplayName = username;
				md.Count = countnums;
				tmplist.add(md);				
			}
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.out.println("Failed to print user count. "
					+ e.getClass().getName() + ": " + e.getMessage());
		}
	}


	// String parseFieldFromLine(String line, String key) {
	// 	// We're looking for a thing that looks like:
	// 	// [key]="[value]"
	// 	// as part of a larger String.
	// 	// We are given [key], and want to return [value].

	// 	// Find the start of the pattern
	// 	String keyPattern = key + "=\"";
	// 	int idx = line.indexOf(keyPattern);

	// 	// No match
	// 	if (idx == -1) return "";

	// 	// Find the closing quote at the end of the pattern
	// 	int start = idx + keyPattern.length();

	// 	int end = start;
	// 	while (line.charAt(end) != '"') {
	// 		end++;
	// 	}

	// 	// Extract [value] from the overall String and return it
	// 	return line.substring(start, end);
	// }

    // Keep track of all users
	// ArrayList<User> users;

	// void readUsers(String filename) throws FileNotFoundException, IOException {
	// 	BufferedReader b = new BufferedReader(
	// 		new InputStreamReader(new FileInputStream(filename), Charset.forName("UTF-8")));
	// 	String line;
	// 	while ((line = b.readLine()) != null) {
	// 		User u = new User();
	// 		u.Id = parseFieldFromLine(line, "Id");
	// 		u.DisplayName = parseFieldFromLine(line, "DisplayName");
	// 		users.add(u);
	// 	}
	// }

    // Keep track of all posts
	// ArrayList<Post> posts;

	// void readPosts(String filename) throws FileNotFoundException, IOException {
	// 	BufferedReader b = new BufferedReader(
	// 		new InputStreamReader(new FileInputStream(filename), Charset.forName("UTF-8")));
	// 	String line;
	// 	while ((line = b.readLine()) != null) {
	// 		Post p = new Post();
	// 		p.Id = parseFieldFromLine(line, "Id");
	// 		p.PostTypeId = parseFieldFromLine(line, "PostTypeId");
	// 		p.OwnerUserId = parseFieldFromLine(line, "OwnerUserId");
	// 		posts.add(p);
	// 	}
	// }

	// User findUser(String Id) {
	// 	for (User u : users) {
	// 		if (u.Id.equals(Id)) {
	// 			return u;
	// 		}
	// 	}
	// 	return new User();
	// }

    

	// public void run() throws FileNotFoundException, IOException {
	// 	// Load our data
	// 	readUsers("users-short.xml");
	// 	readPosts("posts-short.xml");

	// 	// Calculate the users with the most questions
	// 	Map<String, MapData> questions = new HashMap<String, MapData>();

	// 	for (int i = 0; i < posts.size(); i++) {
	// 		Post p = posts.get(i);
	// 		User u_p = findUser(p.OwnerUserId);
	// 		if (questions.get(u_p.Id) == null) { questions.put(u_p.Id, new MapData()); }
	// 		questions.get(u_p.Id).DisplayName = u_p.DisplayName;
	// 		if (p.PostTypeId.equals("1")) { questions.get(u_p.Id).Count ++; }
	// 	}

	// 	System.out.println("Top 10 users with the most questions:");
	// 	for (int i = 0; i < 10; i++) {
	// 		String key = "";
	// 		MapData max_data = new MapData();
	// 		max_data.DisplayName = "";
	// 		max_data.Count = 0;

	// 		for (Map.Entry<String, MapData> it : questions.entrySet()) {
	// 			if (it.getValue().Count >= max_data.Count) {
	// 				key = it.getKey();
	// 				max_data = it.getValue();
	// 			}
	// 		}
        
	// 		questions.remove(key);

	// 		System.out.print(max_data.Count);
	// 		System.out.print('\t');
	// 		System.out.println(max_data.DisplayName);
	// 	}

	// 	System.out.println();
	// 	System.out.println();

	// 	// Calculate the users with the most answers
	// 	Map<String, MapData> answers = new HashMap<String, MapData>();;

	// 	for (int i = 0; i < posts.size(); i++) {
	// 		Post p = posts.get(i);
	// 		User u_p = findUser(p.OwnerUserId);
	// 		if (answers.get(u_p.Id) == null) { answers.put(u_p.Id, new MapData()); }
	// 		answers.get(u_p.Id).DisplayName = u_p.DisplayName;
	// 		if (p.PostTypeId.equals("2")) { answers.get(u_p.Id).Count ++; }
	// 	}

	// 	System.out.println("Top 10 users with the most answers:");
	// 	for (int i = 0; i < 10; i++) {
	// 		String key = "";
	// 		MapData max_data = new MapData();
	// 		max_data.DisplayName = "";
	// 		max_data.Count = 0;

	// 		for (Map.Entry<String, MapData> it : answers.entrySet()) {
	// 			if (it.getValue().Count >= max_data.Count) {
	// 				key = it.getKey();
	// 				max_data = it.getValue();
	// 			}
	// 		}
        
	// 		answers.remove(key);

	// 		System.out.print(max_data.Count);
	// 		System.out.print('\t');
	// 		System.out.println(max_data.DisplayName);
	// 	}
	// }

	public static void main(String[] args) throws FileNotFoundException, IOException {
		try {
			Class.forName("org.sqlite.JDBC");
			Connection c = DriverManager.getConnection("jdbc:sqlite:test.db"); // create db test
			Statement stmt = c.createStatement();
			// Create table QUESTION
			String sql = "DROP TABLE IF EXISTS QUESTION;"
					+ "CREATE TABLE QUESTION "
					+ "(ID 			   INT PRIMARY KEY     NOT NULL,"
					+ " POSTTYPEID     INT    NOT NULL, "
					+ " OWNERUSERID    INT     NOT NULL)";
			stmt.executeUpdate(sql);

			// Create table ANSWER
			sql = "DROP TABLE IF EXISTS ANSWER;" 
					+ "CREATE TABLE ANSWER "
					+ "(ID 			   INT PRIMARY KEY     NOT NULL,"
					+ " POSTTYPEID     INT    NOT NULL, "
					+ " OWNERUSERID    INT     NOT NULL)";
			stmt.executeUpdate(sql);

			// Create table USER
			sql = "DROP TABLE IF EXISTS USER;" 
					+ "CREATE TABLE USER "
					+ "(ID 			   INT PRIMARY KEY     NOT NULL,"
					+ " USERNAME     VCHAR    NOT NULL)";
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database and create tables successfully.");

		// Start parse file and calculate.
		String postFileName = args.length == 2 ? args[0] : "posts-short.xml";
		String userFileName = args.length == 2 ? args[1] : "users-short.xml";
		ParseXml s = new ParseXml();
		
		// Parse File
		System.out.println("Start parsing post, please wait.");
		s.parsePost(postFileName);
		System.out.println("Parse post successfully.");
		System.out.println("Start parsing user, please wait.");
		s.parseUser(userFileName);
		System.out.println("Parse user successfully.");
		System.out.println("Top 10 users with the most questions:");
		s.printUserCount(1);
		System.out.println();
		System.out.println("Top 10 users with the most answers:");
		s.printUserCount(2);
		System.out.println();

		//s.run();
	}

}
