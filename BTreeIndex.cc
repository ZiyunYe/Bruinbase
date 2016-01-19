/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <string.h>
//#include <iostream> //for test

using namespace std;

/*
 * BTreeIndex constructor
 */
 
  PageId   rootPid;    /// the PageId of the root node
  int      treeHeight; /// the height of the tree
  int level;
  PageId parentpid;
  PageId currentpid;
  const int sorpid = sizeof(rootPid);  //size of rootPid
  const int sotreeh = sizeof(treeHeight);  //size of treeHeight
  const int sotreebuf = sorpid+sotreeh;  //size of treeHeight used to store rootPid and treeHeight
  const int max_key_num = floor((PageFile::PAGE_SIZE - soi - sopid)/soent);  //the maximum number of keys in a node
  const int max_key_num_non = floor((PageFile::PAGE_SIZE - soi - sopid)/(soi + sopid));  //the maximum number of keys in a non-leaf node
 
BTreeIndex::BTreeIndex()
{
    rootPid = -1;   //-1 means that the tree is empty
    treeHeight = 0;//0 means that the tree is empty
    level=1;    //the current level of tree during insert, stored in page with pid=0 and after treeHeight
    parentpid=-1; //pid of upper level node
    currentpid=rootPid; //pid of current node, which should start from rootPid
    for(int i=0;i<sotreebuf;i++)   //clear up tree_buffer, which is used to store rootPid, treeHeight, level, parentpid and currentpid
    tree_buffer[i]=0;
	memcpy(tree_buffer+sorpid+sotreeh, &level, sizeof(level));
 	memcpy(tree_buffer+sorpid+sotreeh+sizeof(level), &parentpid, sizeof(parentpid));
 	memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
	pf.write(0, tree_buffer);
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
		int result;
		if((result = pf.open(indexname, mode))<0)   //open the index file and return the error code if possible
			return result;

		//read the information of the tree, including rootPid and treeheight, from pid=0
		int readresult;
		readresult=pf.read(0,tree_buffer);
		
		if((readresult<0)&&(pf.endPid()!=0)) //cannot read the page with pid=0, but the page is not empty
			return readresult;   //return the error code
		memcpy(&rootPid, tree_buffer, sorpid);
		memcpy(&treeHeight, tree_buffer + sorpid, sotreeh);
		memcpy(&currentpid, tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), sizeof(currentpid));
		currentpid=rootPid; //During insert, pid shall start from rootPid
		memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));

		if(treeHeight == 0)  //the tree is empty
			rootPid = -1;
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    //save the rootPid and treeHeight to tree_buffer, then save it to page with pid=0
    memcpy(tree_buffer, &rootPid, sorpid);
    memcpy(tree_buffer + sorpid, &treeHeight, sotreeh);
    pf.write(0, tree_buffer);
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
		int result;
		int temp_siblingKey;
		if(treeHeight == 0)  //the tree is empty
			{
				memcpy(tree_buffer, &rootPid, sorpid);
				memcpy(tree_buffer + sorpid, &treeHeight, sotreeh);
				if((result=pf.write(pf.endPid(),tree_buffer))<0)
					return result;
				BTLeafNode ln; // create the first node
				if((result=ln.insert(key,rid))<0)
					return result;
				rootPid=1;
				if((result=ln.write(rootPid,pf))<0)  //write the first node back to page
				{
					return result;
				}
				treeHeight = 1; //update treeHeight and rootPid in page
				memcpy(tree_buffer, &rootPid, sorpid);
                memcpy(tree_buffer + sorpid, &treeHeight, sotreeh);
				currentpid=rootPid; //During insert, pid shall start from rootPid
				memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
                pf.write(0, tree_buffer);

			}
		else if (treeHeight == 1) // there is only one leaf node(root node)
			{
				BTLeafNode ln;
				ln.read(1,pf);   //read the root node
				if(ln.getKeyCount()<max_key_num)   //there is enough space for this (key, RecordId) pair
					{
						if((result=ln.insert(key,rid))<0)
							return result;
						if((result=ln.write(1,pf))<0)  //write back to page
							return result;
					}
				else
					{
						BTLeafNode sibling; //create the sibling node
						int siblingKey;
						if((result=ln.insertAndSplit(key,rid,sibling,siblingKey))<0)
							return result;
						int siblingpid=2; //pid of the sibling node
						ln.setNextNodePtr(siblingpid);  //the last pointer in original leaf node points to its sibling node
						if((result=ln.write(1,pf))<0)  //write original leaf node back to page
							return result;
						if((result=sibling.write(siblingpid,pf))<0)  //write sibling node back to page
							return result;
						rootPid=3;
						treeHeight=2; //update treeHeight and rootPid in page
                        memcpy(tree_buffer, &rootPid, sorpid);
                        memcpy(tree_buffer + sorpid, &treeHeight, sotreeh);
						currentpid=rootPid; //During insert, pid shall start from rootPid
				        memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
                        pf.write(0, tree_buffer);
						//create new root
						BTNonLeafNode nln;
						nln.initializeRoot(1,siblingKey,siblingpid);
						if((result=nln.write(rootPid,pf))<0)  //write new rootNode back to page
							return result;
					}

				}
		else
			//treeHight more than 2, need recursion
			{
				//read the current level, parentpid and currentpid from page with pid=0
				int readresult;
                readresult=pf.read(0,tree_buffer);
                memcpy(&level, tree_buffer+sorpid+sotreeh, sizeof(level));
                memcpy(&parentpid, tree_buffer+sorpid+sotreeh+sizeof(level), sizeof(parentpid));
                memcpy(&currentpid, tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), sizeof(currentpid));
				//when reach the leaf node level
				if(level == treeHeight)
					{
						BTLeafNode ln;
						ln.read(currentpid,pf);   //read node with the currentpid
						if(ln.getKeyCount()<max_key_num)   //there is enough space for this (key, RecordId) pair
						{
							if((result=ln.insert(key,rid))<0)
								return result;
							if((result=ln.write(currentpid,pf))<0)  //write back to page
								return result;
							currentpid=rootPid;
							//write the current level, parentpid and currentpid to page with pid=0
							memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
							pf.write(0, tree_buffer);
							return 0;
						}
						else // the node is full
						{
							BTLeafNode sibling;
							PageId temp_pid;
							int siblingKey;
							if((result=ln.insertAndSplit(key,rid,sibling,siblingKey))<0)  //need
								return result;
                            int siblingpid=pf.endPid();
							temp_pid=ln.getNextNodePtr();
							ln.setNextNodePtr(siblingpid);  //set the pointer in current leaf node to point to its sibling node
							sibling.setNextNodePtr(temp_pid);  // set the pointer in sibling leaf node
							if((result=ln.write(currentpid,pf))<0)  //write current leaf node back to page
								return result;
							if((result=sibling.write(siblingpid,pf))<0)  //write sibling node back to page
								return result;
							currentpid=rootPid;
							memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
							pf.write(0, tree_buffer);
							return siblingKey; //return the siblingKey to update the non-leaf node
						}
					}
				else
					{
						BTNonLeafNode nln;
						PageId childpid;
						if((result=nln.read(currentpid,pf))<0)
							return result;
						if((result=nln.locateChildPtr(key,childpid))<0)
							return result;
						level=level+1;
						parentpid=currentpid;
						currentpid=childpid;
						//write the current level, parentpid and currentpid to page with pid=0
						memcpy(tree_buffer+sorpid+sotreeh, &level, sizeof(level));
 						memcpy(tree_buffer+sorpid+sotreeh+sizeof(level), &parentpid, sizeof(parentpid));
 						memcpy(tree_buffer+sorpid+sotreeh+sizeof(level)+sizeof(parentpid), &currentpid, sizeof(currentpid));
                        pf.write(0, tree_buffer);
                        if((temp_siblingKey = insert(key,rid))<0)//recursion and temp_siblingKey is siblingKey from lower level
                            return -1;
						level=level-1;
						memcpy(tree_buffer+sorpid+sotreeh, &level, sizeof(level));
						pf.write(0, tree_buffer);
						if(temp_siblingKey!=0)
						{	
						
							if(nln.getKeyCount()<max_key_num_non) //there is enough space for this temp_siblingKey
							{
								if((result=nln.insert(temp_siblingKey,pf.endPid()-1))<0)
									return result;
								if((result=nln.write(parentpid,pf))<0)  //write back to page
									return result;
								return 0;
							}
							else //when the non-leaf node with currentpid is full
							{
								BTNonLeafNode siblingnln;
									int siblingKeynln;
								if((result=nln.insertAndSplit(temp_siblingKey,pf.endPid()-1,siblingnln,siblingKeynln))<0)
									return result;
								if((result=nln.write(parentpid,pf))<0)  //write original node back to page
									return result;
								int siblingnlnpid=pf.endPid();
								if((result=siblingnln.write(siblingnlnpid,pf))<0)  //write sibling back to page
									return result;
								if(level>1)
								{
									return siblingKeynln;
								}
								else // when it is root node
								{
									//create new root
									BTNonLeafNode nln;
									nln.initializeRoot(parentpid,siblingKeynln,siblingnlnpid);
									rootPid=pf.endPid();
									if((result=nln.write(rootPid,pf))<0)  //write new rootNode back to page
										return result;
									treeHeight=treeHeight+1; //update treeHeight and rootPid in page
									memcpy(tree_buffer, &rootPid, sorpid);
									memcpy(tree_buffer + sorpid, &treeHeight, sotreeh);
									return 0;
								}
							}
						}
					}
			}

    return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
		int result;
		if(treeHeight == 0)  //empty tree
		{
			return RC_NO_SUCH_RECORD;
		}
		else if(treeHeight == 1)  //when root node is the leaf node
			{
				BTLeafNode ln;
				if((result=ln.read(rootPid,pf))<0)  //read page file
					return result;
				if((result=ln.locate(searchKey,cursor.eid))<0)   //locate searchKey
					return result;
				cursor.pid=rootPid;
			}
		else  //when treeHeight is larger than 1, start from root node
			{
				BTNonLeafNode nln;
				int pid=rootPid;
				for(int i=1;i<treeHeight;i++)   //travesal the tree to leaf node
				{
					if((result=nln.read(pid,pf))<0)  //read page file
					return result;
					if((result=nln.locateChildPtr(searchKey,pid))<0)   //locate searchKey
					return result;
				}
				BTLeafNode ln;   //when reach leaf node
				if((result=ln.read(pid,pf))<0)  //read page file
					return result;
				if((result=ln.locate(searchKey,cursor.eid))<0)   //locate searchKey
					return result;
				cursor.pid=pid;
				
			}
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	  int result;
	  BTLeafNode ln;
      if((result=ln.read(cursor.pid,pf))<0)  //read page file specified by the index cursor
          return result;
      if(cursor.eid<ln.getKeyCount())   //the entry is in this node
          {
              if((result=ln.readEntry(cursor.eid,key,rid))<0)  //read the (key, rid) pair
				  return result;
              cursor.eid++;   //move foward the cursor to the next entry
              return 0;
          }
		//cursor.eid=ln.getKeyCount(), which means the entry is not in this node, move the cursor to next one
      else //the entry is not in this node
      {
        if(ln.getNextNodePtr()==0) //reach the last leaf node in the tree
          return RC_END_OF_TREE;
		cursor.pid=ln.getNextNodePtr();  // move the cursor to next one in next page
		cursor.eid=0;
		if((result=ln.read(cursor.pid,pf))<0)  //read page file specified by the index cursor
          return result;
		if((result=ln.readEntry(cursor.eid,key,rid))<0)  //read the (key, rid) pair
				  return result;
		cursor.eid++;   //move foward the cursor to the next entry 
		return 0;
      }
}


//for test
/*RC BTreeIndex::show(BTreeIndex &bindex)
{
	cout<<"Here comes the B+ Tree!!!!!!!!!!!!"<<endl;//for test
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
	IndexCursor cursor;
	bindex.locate(0,cursor);
	int key;
	key = 0;
	int i;
	int sum;
	int result;
	sum = 1000;
	cout<<"This is the No.1 leaf node: "<<endl;//for test
	//cin>>x;//for test
	for (i=0;i < 100; i++) 
	{
	if((result=bindex.readForward(cursor,key,rid))==0)  //read the (key, rid) pair
		{
			cout<<"key: "<<key<<"   rid pid: "<<rid.pid<<" sid: "<<rid.sid<<"NUM: "<<i<<endl;
		}
	else 
		cout<<"wrong index node !!!"<<endl;
	}
	
	cout<<"This is the No.2 leaf node: "<<endl;//for test
	//cin>>x;//for test
	for (i=100;i < sum; i++) 
	{
	if((result=bindex.readForward(cursor,key,rid))==0)  //read the (key, rid) pair
		{
			cout<<"key: "<<key<<"   rid pid: "<<rid.pid<<" sid: "<<rid.sid<<"NUM: "<<i<<endl;
		}
	else 
		cout<<"wrong index node !!!"<<endl;
	}
	
	return 0;
}*/
