/*
using FL state synthetic population -- person file
only need to use vector of vector storage system

methods to test
1. existing system in ABC code (line-by-line)
2. reading SQL database directly
3. slurpping
4. memory mapped file
5. UNIX wc-like method
6. reading binary file directly
*/
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <chrono>
#include <sqlite3.h>
#include <bitset>
#include <map>

using namespace std;

// function used in the SQL interactions
static int callback(void *NotUsed, int argc, char **argv, char **azColName){
	int i;
	for(i=0; i<argc; i++){
		cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << endl;
	}
	cout << endl;
	return 0;
}

// method 1: existing system in ABM code (line-by-line)
string populationFilePath = "./sim_pop-florida/population-florida.txt";

vector<vector<int>> ABM_method(){
	vector<vector<int>> popVector;

	// open population text file
	ifstream iss(populationFilePath);

	if(!iss){
		cerr << "ERROR: " << populationFilePath << " not found." << endl;
		popVector.clear();
    return popVector;
  }
  else{
		cout << "Text file opened successfully." << endl;
  }

	// create line buffer for data parsing
  string buffer;

  // int agecounts[NUM_AGE_CLASSES];
  // for (int i=0; i<NUM_AGE_CLASSES; i++) agecounts[i] = 0;

	// stringstream to pass the line into from the lineBuffer
	// this allows the string to be cast to the 5 field integers
  istringstream line;

	// 5 field integer variables
  // per IPUMS, expecting 1 for male, 2 for female for sex
  int id, hid, age, sex, did;
	// empstat;

  // progresses down each line of the text file
  while(getline(iss,buffer)){
		line.clear();
    line.str(buffer);
    /*
      pid home_id sex age day_id
      0 2307965 2 18 574340
      1 2307966 2 54 563454
      2 2307967 2 18 560345
      3 2307968 2 23 563844
      4 2307968 1 44 569230
      5 2307969 1 25 566095
    */
		vector<int> person;

		// only cpatures the lines that have values matching the given pattern (ignores the header)
    if(line >> id >> hid >> sex >> age >> did){
			// store each line (person) in a single vector
      person.clear();
      person.push_back(id);
      person.push_back(hid);
      person.push_back(sex);
      person.push_back(age);
      person.push_back(did);

			// store the person vector in the population vector
      popVector.push_back(person);
    }
  }
  iss.close();

  return popVector;
}

// method 2: reading SQL database directly
string sql_populationFilePath = "./sim_pop-florida-3.0/sim_pop-florida-3.0.sqlite";
string sql_updatedPopulationFilePath = "./sim_pop-florida-3.0/sim_pop-florida-3.0_updated.sqlite";

vector<vector<int>> SQL_method(){
	vector<vector<int>> popVector;

  // necessary elements to create link with SQLite database
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  sqlite3_stmt *stmt;

  // open vim tab spacingconnection with florida (3.0) SQLite database
  rc = sqlite3_open(sql_populationFilePath.c_str(), &db);

  if( rc ) {
		cout << "Can't open database: " << sqlite3_errmsg(db) << endl;
    popVector.clear();
    return popVector;
	}
	else{
		cout << "Opened database successfully." << endl;
  }

  // SQL query to select the necessary fields for each person
  string sql_selectPeople = "SELECT p.pid, r.locid AS home_id, p.sex, p.age, m.locid AS day_id FROM pers AS p LEFT JOIN movement AS m ON p.pid = m.pid LEFT JOIN reside AS r ON p.pid = r.pid;";

  // submit the SQL query to the database
  rc = sqlite3_prepare_v2(db, sql_selectPeople.c_str(), sql_selectPeople.length(), &stmt, nullptr);

  if(rc != SQLITE_OK){
		cout << "SQL Error: " << sqlite3_errmsg(db) << endl;
    sqlite3_free(zErrMsg);
    popVector.clear();
		return popVector;
  }

  vector<int> person;

  // process each selected row from the query
  while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		// create vector for each person
    person.clear();
    person.push_back(sqlite3_column_int(stmt, 0));
    person.push_back(sqlite3_column_int(stmt, 1));
    person.push_back(sqlite3_column_int(stmt, 2));
    person.push_back(sqlite3_column_int(stmt, 3));
    person.push_back(sqlite3_column_int(stmt, 4));

    // store each person's vector in the population vector
    popVector.push_back(person);
	}

	// terminate connection with the SQLite database
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  return popVector;
}

// updated to use DB with primary keys
vector<vector<int>> SQL_methodUpdated(){
  vector<vector<int>> popVector;

  // necessary elements to create link with SQLite database
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  sqlite3_stmt *stmt;

  // open connection with florida (3.0) SQLite database
	rc = sqlite3_open(sql_updatedPopulationFilePath.c_str(), &db);

  if( rc ) {
		cout << "Can't open database: " << sqlite3_errmsg(db) << endl;
    popVector.clear();
    return popVector;
  }
	else{
		cout << "Opened database successfully." << endl;
	}

	// SQL query to select the necessary fields for each person
  string sql_selectPeople = "SELECT p.pid, r.locid AS home_id, p.sex, p.age, m.locid AS day_id FROM pers AS p LEFT JOIN movement AS m ON p.pid = m.pid LEFT JOIN reside AS r ON p.pid = r.pid;";

  // submit the SQL query to the database
  rc = sqlite3_prepare_v2(db, sql_selectPeople.c_str(), sql_selectPeople.length(), &stmt, nullptr);

  if(rc != SQLITE_OK){
		cout << "SQL Error: " << sqlite3_errmsg(db) << endl;
    sqlite3_free(zErrMsg);
    popVector.clear();
    return popVector;
	}

  vector<int> person;

  // process each selected row from the query
  while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		// create vector for each person
    person.clear();
    person.push_back(sqlite3_column_int(stmt, 0));
    person.push_back(sqlite3_column_int(stmt, 1));
    person.push_back(sqlite3_column_int(stmt, 2));
    person.push_back(sqlite3_column_int(stmt, 3));
    person.push_back(sqlite3_column_int(stmt, 4));

    // store each person's vector in the population vector
    popVector.push_back(person);
	}

  // terminate connection with the SQLite database
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  return popVector;
}

// first attempt to slurp/memory map the entire file and read line-by-line from memory
// uses stringstream to memory map
vector<vector<int>> slurp_method(){
	vector<vector<int>> popVector;

  // open population text file
  ifstream iss(populationFilePath);

  if (!iss) {
		cerr << "ERROR: " << populationFilePath << " not found." << endl;
    popVector.clear();
    return popVector;
	}
  else{
		cout << "Text file opened successfully." << endl;
  }

	// memoryBuffer holds the input file in memory
	stringstream memoryBuffer;

	// lineBuffer to hold each line from the file
	string lineBuffer;

	// stringstream to pass the line into from the lineBuffer
	// this allows the string to be cast to the 5 field integers
	stringstream lineStrip;

	// reads the input file into the stringstream
	memoryBuffer << iss.rdbuf();

	// 5 field integer variables
  // per IPUMS, expecting 1 for male, 2 for female for sex
	int id, hid, age, sex, did;

	// progresses down each line of the text file
	while(getline(memoryBuffer, lineBuffer)){
		lineStrip.clear();
		lineStrip.str(lineBuffer);

		vector<int> person;

		if(lineStrip >> id >> hid >> sex >> age >> did){
			// store each line (person) in a single vector
			person.clear();
			person.push_back(id);
			person.push_back(hid);
			person.push_back(sex);
			person.push_back(age);
			person.push_back(did);

			// store the person vector in the population vector
			popVector.push_back(person);
		}
	}

	iss.close();

	return popVector;
}

// second attempt at memory mapping
// maps the file into a string with predetermined memory allocation
vector<vector<int>> slurp_method2(){
	vector<vector<int>> popVector;

  // open population text file
  ifstream iss(populationFilePath);

  if (!iss) {
		cerr << "ERROR: " << populationFilePath << " not found." << endl;
    popVector.clear();
    return popVector;
	}
  else{
		cout << "Text file opened successfully." << endl;
  }

	// allocates a string with memory equal to the input file size
	iss.seekg(0, ios::end);
	size_t size = iss.tellg();
	string buffer(size, ' ');
	iss.seekg(0);
	iss.read(&buffer[0], size);

	// memoryBuffer used to read the mapped file from the string
	// does not copy the contents of the mapped file (according to StackOverflow)
	stringstream memoryBuffer;

	// string to hold each line from the mapped file
	string lineBuffer;

	// stringstream to pass the line into from the lineBuffer
	// this allows the string to be cast to the 5 field integers
	stringstream lineStrip;

	// 5 field integer variables
  // per IPUMS, expecting 1 for male, 2 for female for sex
	int id, hid, age, sex, did;

	// I believe that this allows the memoryBuffer stringstream to read over the contents of the mapped file without copying it
  memoryBuffer.rdbuf()->pubsetbuf(&buffer[0], size);

	// progresses down each line of the text file
	while(getline(memoryBuffer, lineBuffer)){
		lineStrip.clear();
		lineStrip.str(lineBuffer);

		vector<int> person;

		if(lineStrip >> id >> hid >> sex >> age >> did){
			// store each line (person) in a single vector
			person.clear();
			person.push_back(id);
			person.push_back(hid);
			person.push_back(sex);
			person.push_back(age);
			person.push_back(did);

			// store the person vector in the population vector
			popVector.push_back(person);
		}
	}

	iss.close();
	buffer.clear();

	return popVector;
}

int main(){
	vector<vector<int>> population;
  population.clear();

	map<string, vector<double>> methodTimes;
	int testReplicates = 10;

  // time ABM method
	vector<double> ABM_times;
	for(int i = 0; i < testReplicates; i++){
		cout << "ABM method --- replicate " << i << "\n";

	  auto start1 = chrono::steady_clock::now();
	  population = ABM_method();
	  auto end1 = chrono::steady_clock::now();
	  auto diff1 = end1 - start1;

	  cout << "Replicate time: " << chrono::duration <double, milli> (diff1).count() << " ms" << "\n";
		ABM_times.push_back(chrono::duration <double, milli> (diff1).count());

	  cout << "Population size: " << population.size() << "\n" << "\n";
	  population.clear();
	}
	methodTimes.insert(pair<string, vector<double>>("ABM", ABM_times));

  // time SQL method (indices, no primary keys)
	vector<double> SQL_times;
	for(int i = 0; i < testReplicates; i++){
		cout << "SQL method --- replicate " << i << "\n";

	  auto start2 = chrono::steady_clock::now();
	  population = SQL_method();
	  auto end2 = chrono::steady_clock::now();
	  auto diff2 = end2 - start2;

	  cout << "Replicate time: " << chrono::duration <double, milli> (diff2).count() << " ms" << endl;
		SQL_times.push_back(chrono::duration <double, milli> (diff2).count());

	  cout << "Population size: " << population.size() << "\n" << "\n";
	  population.clear();
	}
	methodTimes.insert(pair<string, vector<double>>("SQL", SQL_times));

	// time SQL method 2 (indices and primary keys)
	vector<double> SQL_update_times;
	for(int i = 0; i < testReplicates; i++){
		cout << "SQL method (updated with primary keys) --- replicate " << i << "\n";

	  auto start5 = chrono::steady_clock::now();
	  population = SQL_methodUpdated();
	  auto end5 = chrono::steady_clock::now();
	  auto diff5 = end5 - start5;

	  cout << "Replicate time: " << chrono::duration <double, milli> (diff5).count() << " ms" << endl;
		SQL_update_times.push_back(chrono::duration <double, milli> (diff5).count());

	  cout << "Population size: " << population.size() << "\n" << "\n";
	  population.clear();
	}
	methodTimes.insert(pair<string, vector<double>>("SQL_updated", SQL_update_times));


	// time slurp/memory map method 1 (into stringstream)
	vector<double> slurp1_times;
	for(int i = 0; i < testReplicates; i++){
		cout << "Slurp 1 method --- replicate " << i << "\n";

	  auto start3 = chrono::steady_clock::now();
	  population = slurp_method();
	  auto end3 = chrono::steady_clock::now();
	  auto diff3 = end3 - start3;

	  cout << "Replicate time: " << chrono::duration <double, milli> (diff3).count() << " ms" << endl;
		slurp1_times.push_back(chrono::duration <double, milli> (diff3).count());

		cout << "Population time: " << population.size() << "\n" << "\n";
	  population.clear();
	}
	methodTimes.insert(pair<string, vector<double>>("slurp1", slurp1_times));

	// time slurp/memory map method 2 (into string)
	vector<double> slurp2_times;
	for(int i = 0; i < testReplicates; i++){
		cout << "Slurp 2 method --- replicate " << i << "\n";

	  auto start4 = chrono::steady_clock::now();
	  population = slurp_method2();
	  auto end4 = chrono::steady_clock::now();
	  auto diff4 = end4 - start4;

	  cout << "Replicate time: " << chrono::duration <double, milli> (diff4).count() << " ms" << endl;
		slurp2_times.push_back(chrono::duration <double, milli> (diff4).count());

		cout << "Population size: " << population.size() << "\n" << "\n";
	  population.clear();
	}
	methodTimes.insert(pair<string, vector<double>>("slurp2", slurp2_times));

	// print average method times (in ms) for each method
	cout << "---------------------------------------------------------------------------\n";
	cout << "Average Method Times (in ms) for " << testReplicates << " Replicates per Method\n";
	for(auto& methodMap : methodTimes){
		double methodTimeSum = 0;
		for(auto& replicate : methodMap.second){
			methodTimeSum += replicate;
		}
		cout << methodMap.first << ' ' << (methodTimeSum/(methodMap.second.size())) << "\n";
	}
}
