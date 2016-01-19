/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void); 


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
	BTreeIndex bindex;
	IndexCursor cursor;
	
  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  
  if((bindex.open(table + ".idx", 'r')) == 0)  //index file can be opened, use the index to select
  	{
  		int locatekey=-1;
  		int stopkey=-1;
  		int num_of_ne=0;  //count the number of non-equal conditions
  		int flag_count_value=0;   //if select count(*) from table where value, flag=1

  		//check the conditions in cond
  		for(int i=0;i<cond.size();i++)
  		{
  			//if it is non-equal condition, count it. Except 'Select key from table where key<>10' or 'Select count(*) from table where key<>10'
  			//because here we can use index 
  			//and it does not need to read table. Otherwise, if the conditon is non-equal, we should not use index, just read table directly
  			if( (cond[i].comp == SelCond::NE) && (((attr!=1) && (attr!=4)) || (cond[i].attr !=1)) )
  				num_of_ne=num_of_ne+1;  //not using index
  				
  			if((cond[i].attr == 2)&&(attr==4))
  				flag_count_value=1;
  				
  			if(cond[i].attr != 1)  //this condition is not about the key, so we do not need to use index
  				continue;
  			
  			if(cond[i].comp == SelCond::EQ) //this condition is key equals to a value, we just need to check this condtion
  			{
  				locatekey = atoi(cond[i].value);
				stopkey = atoi(cond[i].value);
  				break;
  			}
  			
  			if(cond[i].comp == SelCond::GT) //we need to make sure that the locatekey is larger than the key 
  				{
  					if(locatekey<=atoi(cond[i].value))
  						locatekey=atoi(cond[i].value)+1;
  				}
  				
  			if(cond[i].comp == SelCond::GE) //we need to make sure that the locatekey is not smaller than the key
  				{
  					if(locatekey<atoi(cond[i].value))
  						locatekey=atoi(cond[i].value);
  				}
  				
  			if(cond[i].comp == SelCond::LT) //we need to set up stopkey to stop the readForward function
  				{
  					if((stopkey>=atoi(cond[i].value)||(stopkey==-1)))
  						stopkey=atoi(cond[i].value)-1;
  				}   
  				
  			if(cond[i].comp == SelCond::LE) //we need to set up stopkey to stop the readForward function
  				{
  					if((stopkey>atoi(cond[i].value)||(stopkey==-1)))
  						stopkey=atoi(cond[i].value);
  				}
  			
  			 			
       }
        
       
       //if there are only required non-equal conditions(except select key where key), read underlying table directly, do not use index
        
        if((num_of_ne==cond.size())&&(cond.size()!=0))
      			{
      				//no index file, read normally
		while (rid < rf.endRid())
		{
			// read the tuple
			if ((rc = rf.read(rid, key, value)) < 0) 
			{
				fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				goto exit_select3;
			}
			// check the conditions on the tuple
			for (unsigned i = 0; i < cond.size(); i++) 
			{
				// compute the difference between the tuple value and the condition value
				switch (cond[i].attr) 
				{
					case 1:
						diff = key - atoi(cond[i].value);
						break;
					case 2:
						diff = strcmp(value.c_str(), cond[i].value);
						break;
				}

				// skip the tuple if any condition is not met
				switch (cond[i].comp) 
				{
					case SelCond::EQ:
						if (diff != 0) goto next_tuple1;
						break;
					case SelCond::NE:
						if (diff == 0) goto next_tuple1;
						break;
					case SelCond::GT:
						if (diff <= 0) goto next_tuple1;
						break;
					case SelCond::LT:
						if (diff >= 0) goto next_tuple1;
						break;
					case SelCond::GE:
						if (diff < 0) goto next_tuple1;
						break;
					case SelCond::LE:
						if (diff > 0) goto next_tuple1;
						break;
				}
			}

			// the condition is met for the tuple. 
			// increase matching tuple counter
			count++;

			// print the tuple 
			switch (attr) 
			{
				case 1:  // SELECT key
					fprintf(stdout, "%d\n", key);
					break;
				case 2:  // SELECT value
					fprintf(stdout, "%s\n", value.c_str());
					break;
				case 3:  // SELECT *
					fprintf(stdout, "%d '%s'\n", key, value.c_str());
					break;
			}

			// move to the next tuple
			next_tuple1:
				++rid;
		}

		 // print matching tuple count if "select count(*)"
		 if (attr == 4) {
			fprintf(stdout, "%d\n", count);
		  }
		  rc = 0;

	  // close the table file and return
	  exit_select3:
	  rf.close();
	  return rc;
	  }
	
	
	 //For other conditions, i.e. there is at least 1 not non-equal condition, continue using index
        if(locatekey == -1)  //the conditions are not about the key, cannot use btreeindex, just locate(0,cursor);
      	{
      			bindex.locate(0,cursor);
      	}
        else
		{
			bindex.locate(locatekey,cursor);
		}
  	  
  	    while(bindex.readForward(cursor,key,rid) == 0)
  	    { 
			if((key>stopkey)&&(stopkey!=-1))  //do not need to read the entry after this node, break the loop
  				break;
  			
  		if((attr!=4)||(flag_count_value==1))
  			{	
				if ((rc = rf.read(rid, key, value)) < 0) 
  				{
  					fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
  					goto exit_select;
  				}
  			}
			
  			//check each condition
  			int error_cond=0;  //show if there is an error, unnecessary
  			
  			for(int i=0;i<cond.size();i++)
  			{
  				// compute the difference between the tuple value and the condition value
  				switch (cond[i].attr) {
  				case 1:
  					diff = key - atoi(cond[i].value);
  					break;
  				case 2:
  					diff = strcmp(value.c_str(), cond[i].value);
  					break;
  					}
  				// skip the tuple if any condition is not met
  				switch (cond[i].comp) {
  				case SelCond::EQ:
  					if (diff != 0) {error_cond=1;continue;}   	//error_cond=1 means that this key does not meet the requirement, go to next key
  						break;
  				case SelCond::NE:  //non-equal is special, it devides the interval to 2 parts
  					if (diff == 0) {error_cond=1;continue;}
  						break;
  				case SelCond::GT:
  					if (diff <= 0) {error_cond=1;continue;}
  						break;
  				case SelCond::LT:
  					if (diff >= 0) {error_cond=1;continue;}
  						break;
  				case SelCond::GE:
  					if (diff < 0) {error_cond=1;continue;}
  						break;
  				case SelCond::LE:
  					if (diff > 0) {error_cond=1;continue;}
  						break;
  				}
  			}
  			// the condition is met for the tuple. 
  			// increase matching tuple counter
  			if(error_cond==1) //the condition is non-equal, we need to skip this key
  				goto do_nothing_this_key;
  			
  			count++;
  			
  			// print the tuple 
  			switch (attr) {
				case 1:  // SELECT key
					fprintf(stdout, "%d\n", key);
					break;
				case 2:  // SELECT value
					fprintf(stdout, "%s\n", value.c_str());
					break;
				case 3:  // SELECT *
					fprintf(stdout, "%d '%s'\n", key, value.c_str());
					break;
    		}
    		do_nothing_this_key:  //simply skip this key
    			if(error_cond==1)
    				error_cond=0;   //the error_cond is set back to 0 and can be used for the judgement of following key   				
    	}
  		if (attr == 4)
		{
			fprintf(stdout, "%d\n", count);
		}
		rc = 0;

		// close the table file and return
		exit_select:
		rf.close();
		return rc;				
  	}
  		
  else
  	{
  		//no index file, read normally
		while (rid < rf.endRid())
		{
			// read the tuple
			if ((rc = rf.read(rid, key, value)) < 0) 
			{
				fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				goto exit_select2;
			}
			// check the conditions on the tuple
			for (unsigned i = 0; i < cond.size(); i++) 
			{
				// compute the difference between the tuple value and the condition value
				switch (cond[i].attr) 
				{
					case 1:
						diff = key - atoi(cond[i].value);
						break;
					case 2:
						diff = strcmp(value.c_str(), cond[i].value);
						break;
				}

				// skip the tuple if any condition is not met
				switch (cond[i].comp) 
				{
					case SelCond::EQ:
						if (diff != 0) goto next_tuple;
						break;
					case SelCond::NE:
						if (diff == 0) goto next_tuple;
						break;
					case SelCond::GT:
						if (diff <= 0) goto next_tuple;
						break;
					case SelCond::LT:
						if (diff >= 0) goto next_tuple;
						break;
					case SelCond::GE:
						if (diff < 0) goto next_tuple;
						break;
					case SelCond::LE:
						if (diff > 0) goto next_tuple;
						break;
				}
			}

			// the condition is met for the tuple. 
			// increase matching tuple counter
			count++;

			// print the tuple 
			switch (attr) 
			{
				case 1:  // SELECT key
					fprintf(stdout, "%d\n", key);
					break;
				case 2:  // SELECT value
					fprintf(stdout, "%s\n", value.c_str());
					break;
				case 3:  // SELECT *
					fprintf(stdout, "%d '%s'\n", key, value.c_str());
					break;
			}

			// move to the next tuple
			next_tuple:
				++rid;
		}

		 // print matching tuple count if "select count(*)"
		 if (attr == 4) {
			fprintf(stdout, "%d\n", count);
		  }
		  rc = 0;

	  // close the table file and return
	  exit_select2:
	  rf.close();
	  return rc;
	}
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */

	//open table
	RecordFile lf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
	int num;
	num=0;
	RC lc;
	
	//create the table file
	if ((lc = lf.open(table + ".tbl", 'w')) < 0) {
    fprintf(stderr, "Error: table %s cannot be accessed\n", table.c_str());
    return lc;
  }
  
  //open the record file
	string line;
	int key;
	string value;
	
	ifstream loadtable;
	loadtable.open(loadfile.c_str());
	
	if ((lc =loadtable.is_open())<0) {     //generate an Error when we cannot open a file
    fprintf(stderr, "Error: Cannot open file %s\n", loadfile.c_str());
    goto exit_load;
  }
  
  
  //load to the table  
	while(loadtable.good())
	{
		getline(loadtable, line);
		if(parseLoadLine(line, key, value) == 0)
		{
			lf.append(key, value, rid);      //add file to the table
			if(index == 1)   //insert (key,rid) pair to the index
				{
					BTreeIndex bindex;
					if ((lc = bindex.open(table + ".idx", 'w')) < 0) 
					{
						fprintf(stderr, "Error: index %s cannot be accessed\n", loadfile.c_str());
						goto exit_load;
					}
					bindex.insert(key,rid);
					num++;
					bindex.close();
				}
			rid++;
		}
	}
		
	//close file and table
	exit_load:
	/*//for test
	BTreeIndex b2;
	b2.open(table + ".idx", 'r');
	b2.show(b2); //for test
	b2.close();
	//for test*/
	
	loadtable.close();
	lf.close();
  
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
