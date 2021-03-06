#include "BTreeNode.h"
#include <string.h>
#include <math.h>

using namespace std;

const int max_key_num = floor((PageFile::PAGE_SIZE - soi - sopid)/soent);  //the maximum number of keys in a node
const int max_key_num_non = floor((PageFile::PAGE_SIZE - soi - sopid)/(soi + sopid));  //the maximum number of keys in a non-leaf node
//Using constructor to for initialization
BTLeafNode::BTLeafNode()
{
	for(int i=0;i<PageFile::PAGE_SIZE;i++)
	{
		buffer[i]='\0';
	}
	int temp=0;
	memcpy(buffer,&temp,soi);
}//clear up the buffer and the num_of_keys_in_node in leaf nodes

//Buffer structure: [num_of_keys_in_node(length=int) rid1 key1 rid2 key2...pageid] length=PageFile::PAGE_SIZE


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid,buffer); }   //use the read function in Pagefile to read page into buffer

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid,buffer); }  //use the write function in Pagefile write buffer into page

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ int keycount=0;
	memcpy(&keycount,buffer,soi);
	return keycount;
	}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ //check if there is enough space for the new entry
	if (getKeyCount() == max_key_num)
		return RC_NODE_FULL;

	//find the eid for new entry
	int eid;
	locate(key, eid);
	//make space for new entry
	char temp[(getKeyCount()-eid)*soent];   //move the rest of existed keys
	memcpy(&temp,buffer+soi+eid*soent,(getKeyCount()-eid)*soent);
	memcpy(buffer+soi+(eid+1)*soent,&temp,(getKeyCount()-eid)*soent);

	//insert new entry
	entry ENTRY;
	ENTRY.key=key;
	ENTRY.rid=rid;
	memcpy(buffer+soi+eid*soent,&ENTRY,soent);

	//update the num_of_keys_in_node
	int key_num=getKeyCount();
	key_num=key_num+1;
	memcpy(buffer,&key_num,soi);

	return 0;
	}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid,
                              BTLeafNode& sibling, int& siblingKey)
{ 
	//check if there is enough space for the new entry, if so, we do not need to insert and split
	if (getKeyCount() < max_key_num)
		return -1;
	int halfkey=ceil(max_key_num/2);
	//move half of the keys to the sibling
	for(int i=halfkey;i<max_key_num;i++)
	{
		int newkey;
		RecordId newrid;
		readEntry(i,newkey,newrid);
		sibling.insert(newkey,newrid);
	}

	//update the num_of_keys_in_node in the old node
	memcpy(buffer,&halfkey,soi);

	//insert new entry
	int eid;
	if(locate(key,eid) == 0)  //new entry can be inserted into the old node
	{
		insert(key,rid);
	}
	else
	{
		sibling.insert(key,rid);
	}

	//get the first key in the sibling node after split
	RecordId siblingrid;
	sibling.readEntry(0,siblingKey,siblingrid);

	return 0; }

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 	
	RC key;
	RecordId rid;
	for( int i=0; i<getKeyCount(); i++)
	{
		readEntry(i,key,rid);
		if (key>=searchKey)
		{
			eid=i;
			return 0;
		}
	}
	eid=getKeyCount();  //set eid to the index entry immediately after the largest index key that is smaller than searchKey
	return RC_NO_SUCH_RECORD;   //cannot find the searchKey
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ //if entry eid is negative or larger than the maximum number of keys in this node, return error
	if (eid < 0 || eid > getKeyCount())
	return -1;

	entry ENTRY;
	memcpy(&ENTRY,buffer+soi+soent*eid,soent);
	key=ENTRY.key;
	rid=ENTRY.rid;
	return 0; }

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr()
{ PageId pid;
	memcpy(&pid,buffer+soi+soent*max_key_num,sopid);
	return pid; }

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ memcpy(buffer+soi+soent*max_key_num,&pid,sopid);
	return 0; }

//Non-leaf nodes
//Using constructor to for initialization
BTNonLeafNode::BTNonLeafNode(){
	for(int i=0;i<PageFile::PAGE_SIZE;i++)
	{
		buffer[i]='\0';
	}
	int temp=0;
	memcpy(buffer,&temp,soi);
}//clear up the buffer and the num_of_keys_in_node in leaf nodes


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid,buffer); }   //use the read function in Pagefile to read page into buffer

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid,buffer); }  //use the write function in Pagefile write buffer into page

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ int keycount=0;
	memcpy(&keycount,buffer,soi);
	return keycount;
	}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ //check if there is enough space for the new key
	if (getKeyCount() == max_key_num_non)
		return RC_NODE_FULL;

	//find the eid for new key
	int eid;

	//locate the position of new key
	for(int i=0;i<getKeyCount();i++)
	{
		int temp_key;
		memcpy(&temp_key,buffer+soi+sopid+i*(soi+sopid),soi);
		if(temp_key>=key)
			{eid=i;
			break;
		}
		else
			eid=getKeyCount();
	}

	//make space for new entry
	char temp[(getKeyCount()-eid)*(soi+sopid)];   //move the rest of existed keys
	memcpy(&temp,buffer+soi+sopid+eid*(soi+sopid),(getKeyCount()-eid)*(soi+sopid));
	memcpy(buffer+soi+sopid+(eid+1)*(soi+sopid),&temp,(getKeyCount()-eid)*(soi+sopid));

	//insert new entry
	memcpy(buffer+soi+sopid+eid*(soi+sopid),&key,soi);
	memcpy(buffer+soi+sopid+eid*(soi+sopid)+soi,&pid,sopid);

	//update the num_of_keys_in_node
	int key_num=getKeyCount();
	key_num=key_num+1;
	memcpy(buffer,&key_num,soi);

	return 0;
	}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ //check if there is enough space for the new entry, if so, we do not need to insert and split
	if (getKeyCount() < max_key_num_non)
		return -1;
	int halfkey=ceil(max_key_num_non/2);

	//move half of the keys to the sibling
	for(int i=halfkey;i<max_key_num_non;i++)
	{
		int newkey;
		PageId newpid;
		memcpy(&newkey,buffer+soi+sopid+i*(soi+sopid),soi);
		memcpy(&newpid,buffer+soi+sopid+i*(soi+sopid)+soi,sopid);
		if (sibling.getKeyCount()==0)// initialize the root node
			{
			  PageId newpid_left;
			  memcpy(&newpid_left,buffer+soi+(i*(soi+sopid)),sopid);
			  sibling.initializeRoot(newpid_left,newkey,newpid);
			}
		else
		sibling.insert(newkey,newpid);
	}

	//update the num_of_keys_in_node in the old node
	memcpy(buffer,&halfkey,soi);

	//insert new entry
	int eid;
	for(int i=0;i<getKeyCount();i++)
	{
		int temp_key;
		memcpy(&temp_key,buffer+soi+sopid+i*(soi+sopid),soi);
		if(temp_key>=key)
			{eid=i;
			break;
		}
		else
			eid=getKeyCount();
	}
	if(eid != getKeyCount())  //new entry can be inserted into the old node
		insert(key,pid);
	else
		sibling.insert(key,pid);

	//get the middle key after split
	memcpy(&midKey,buffer+soi+sopid+(getKeyCount()-1)*(soi+sopid),soi);

	return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	PageId pid_left;
	PageId pid_right;
	int key_mid;
	for(int i=0;i<getKeyCount();i++)
	{
		memcpy(&pid_left,buffer+soi+i*(soi+sopid),sopid);
		memcpy(&key_mid,buffer+soi+i*(soi+sopid)+sopid,soi);
		memcpy(&pid_right,buffer+soi+i*(soi+sopid)+sopid+soi,sopid);   //get the [pid_left key_mid pid_right]
		if(key_mid>searchKey)
			{
				pid=pid_left;
				break;
			}
		else
			pid=pid_right;               //if search for the pageid in another node there may be a problem#####
	}
	return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ int keycount=1;
	memcpy(buffer,&keycount,soi);
	memcpy(buffer+soi,&pid1,sopid);
	memcpy(buffer+soi+sopid,&key,soi);
	memcpy(buffer+soi+sopid+soi,&pid2,sopid);   //insert the [pid1 key pid2] to root
	return 0;
	}



