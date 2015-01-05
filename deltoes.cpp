#include <iostream>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <map>
/*
 * This is a simple program for converting a delimited data from a file into 
 * Elasticsearch JSON objects that can be fast loaded using the _bulk insert
 * endpoint.
 *
 * The idea is to inialize the Config object with the required configuration 
 * values iterate over the source file line by line and product JSON objects.
 * 
 *
 */

using namespace std;

/*
 * The object that stores config variables.
 */

class Config {
    string infile, outfile;
    char delimeter;
    bool skipFirstLine;
    map<string,string> meta;
    map<int,string> keys;

  public: 
    Config() : infile("Data.csv")
      ,outfile("Data.csv.out")
      ,delimeter('\277')
      ,skipFirstLine(true)
      ,meta({ {"_index","index_name"}, {"_type","type_name"}, {"_id","wid"} }) 
      ,keys({ {0,"wid"}, {1,"surname"}, {2,"postcode"}, {3,"registration"}, {4,"visitDate"}, {5,"diDate"} })
       {}
    string getInFile() { return infile; };
    string getOutFile() { return outfile; };
    bool getSkipFirstLine() { return skipFirstLine; };
    char getDelimeter() { return delimeter; };
    map<int,string> getKeys() { return keys; }; 
    map<string,string> getMeta() { return meta; }; 
    int countLines() 
      { ifstream fileIn(getInFile());
        string line;
        int count = 0;
        while (getline(fileIn, line)) {count++;};
        fileIn.close();
        return count; };
};

/*
 * Sanitize strings.
 */

string removeChar(string arg, char rem) 
{
  /*
   * std::remove: Removes all elements satisfying specific criteria from the 
   * range [first, last] and returns a past-the-end iterator for the new end
   * of the range
   * 
   * std::string:erase Erases part of the string, reducing its length
   *
   */
  arg.erase(remove(arg.begin(),arg.end(),rem),arg.end());
  return arg; 
}

/*
 * The method that does the actual work.
 */


void convert(Config config) 
{
  int progress = 0;
  string line;
  ifstream infile(config.getInFile());
  ofstream outfile(config.getOutFile());

  map<int,string> keyMap = config.getKeys();
  map<string,string> esMeta = config.getMeta();
  int size = keyMap.size();
  char delimiter = config.getDelimeter();
  bool skipFirst = config.getSkipFirstLine();

  // iterate over every line in the file, \n is used by default
  while (getline(infile, line))
  {
    istringstream iss(removeChar(line,'\r')); 
    int count = 0;
    string token;
    string id;

    // split the line into tokens using the delimeter and iterate
    string partialJson = "";
    while (getline(iss,token,delimiter)) 
    {
      //make sure there are no carriage return characters
      if (count < size)
      {
        // if key name is like the one configured for "_id" get its value
        string mapVal = keyMap[count];
        if ( count  == 2 ) { token = removeChar(token,' '); };  // This is ugly hardcondig
        if ( count  == 5 ) { replace( token.begin(), token.end(), ' ', 'T' ); };  // This is ugly hardcondig
        if ( mapVal == esMeta["_id"] ) { id = token; };
        partialJson.append("\"");
        partialJson.append(mapVal);
        partialJson.append("\":\""); 
        partialJson.append(token);
        partialJson.append("\",");
        count++;
      } else {
        break;
      }
    }
    partialJson = partialJson.substr(0, partialJson.size()-1); // Remove comma

    string partialMeta = "";
    for(map<string,string>::iterator it = esMeta.begin(); it != esMeta.end(); ++it) {
      string key = it->first;
      string value = it->second;
      if (key == "_id") { value = id; }; // This is ugly hardcondig
      partialMeta.append("\"");
      partialMeta.append(key);
      partialMeta.append("\":\"");
      partialMeta.append(value);
      partialMeta.append("\",");
    }
    partialMeta = partialMeta.substr(0, partialMeta.size()-1); // Remove comma

    string json = "{ \"index\": {" + partialMeta + "}}\n" + "{" + partialJson + "}";
    
    if (line != "") {
      if (skipFirst) {
        if (progress != 0) {
          outfile << json << endl;
        }
      }
    } 
    progress++;
    cout << "\r" << progress << flush;
  }
  infile.close();
  outfile << endl;
  outfile.close();
  cout << "	Processing finished." << endl;
}

/*
 * The main method.
 */

int main()
{
  Config config;
  int count = config.countLines();
  cout << "LINES: " << count << endl;
  thread t1(convert,config);
  t1.join();
  return(0);
}
