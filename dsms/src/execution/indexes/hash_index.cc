
/**
 * @file             hash_index.h
 * @date             May 31, 2004
 * @brief            Hash index implementation
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

// 3 layers should suffice for 1 G records
#define MAX_LAYERS      3

using namespace Execution;
using namespace std;

static unsigned int pathToBucket [MAX_LAYERS];

int HashIndex::computeBucketIndexParams() 
{
	// number of bits per layer: floor (log_2 (pageSize / sizeof(void*)))
	unsigned int t = pageSize / (sizeof (void *));
	for (numBitsPerLayer = 0; t > 1 ; numBitsPerLayer++, t /= 2);
	
	// We start off with 1 layer: 
	numLayers = 1;
	numBits = numBitsPerLayer;  // * 1
	
	// numBuckets: 2^numBits;
	numBuckets = (1 << numBits);
	
	return 0;
}
	
int HashIndex::computeDataPageLayoutParams() 
{	
	unsigned int entryAlign;
	
	dataPartOffset = sizeof (char *);
	
	entryAlign = __alignof__ (Entry);
	
	if (dataPartOffset % entryAlign != 0)
		dataPartOffset += (entryAlign - dataPartOffset % entryAlign);
	
	numEntriesPerPage = (pageSize - dataPartOffset) / sizeof (Entry);
	
	return 0;
}

int HashIndex::constructBucketIndex ()
{
	// Allocate page for the buckets.
	if (memMgr -> allocatePage (bucketIndexRoot) != 0) {
		LOG << "HashIndex [" << id << "]: " 
			<< "Unable to allocate page for buckets"			
			<< endl;
		
		ASSERT (0);
		
		return -1;
	}
	
	// All buckets are initially empty 
	memset (bucketIndexRoot, 0, pageSize);
	
	return 0;
}

HashIndex::HashIndex(unsigned int id, ostream & _LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> memMgr = 0;
	this -> bucketIndexRoot = 0;
	this -> freeEntriesList = 0;
	this -> dataPageList = 0;
	this -> evalContext = 0;
	this -> updateHashEval = 0;
	this -> keyEqual = 0;
	this -> scanHashEval = 0;
	this -> iter = 0;

#ifdef _MONITOR_
	this -> numEntries = 0;
#endif
	return;
}

HashIndex::~HashIndex () {
	if (updateHashEval)
		delete updateHashEval;
	if (keyEqual)
		delete keyEqual;
	if (scanHashEval)
		delete scanHashEval;
	if (iter)
		delete iter;
}

int HashIndex::setMemoryManager (MemoryManager *memMgr)
{
	ASSERT (memMgr);

	this -> memMgr = memMgr;
	return 0;
}

int HashIndex::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int HashIndex::setUpdateHashEval (HEval *hashEval)
{
	ASSERT (hashEval);
	
	this -> updateHashEval = hashEval;
	return 0;
}

int HashIndex::setScanHashEval (HEval *hashEval)
{
	ASSERT (hashEval);
	
	this -> scanHashEval = hashEval;
	return 0;
}

int HashIndex::setKeyEqual (BEval *keyEqual)
{
	ASSERT (keyEqual);
	
	this -> keyEqual = keyEqual;
	return 0;
}

int HashIndex::setThreshold (float threshold)
{
	ASSERT (threshold > 0 && threshold < 1.0);

	this -> threshold = threshold;
	return 0;
}

int HashIndex::initialize ()
{
	int rc;
	
	ASSERT (memMgr);
	
	pageSize = memMgr -> getPageSize();
	
	// Work out the initial details (parameters) of the bucket index
	// structure 
	if ((rc = computeBucketIndexParams ()) != 0)
		return rc;
	
	// Work out the details of data page layout
	if ((rc = computeDataPageLayoutParams ()) != 0)
		return rc;
	
	// Construct the bucket index
	if ((rc = constructBucketIndex ()) != 0)
		return rc;
		
	// We do not allocate space for data pages: these will be done @ first
	// insert time.
	freeEntriesList = 0;
	dataPageList = 0;
	
	this -> numNonEmptyBuckets = 0;
	
	NON_ROOT_MASK = ROOT_MASK = ~(~(Hash(0)) << numBitsPerLayer);

	// Create a hash index iterator
	iter = new HashIndexIterator (evalContext, keyEqual);
	
	return 0;
}

#define CHILD(n,i) (*((char **)(n) + i))

#define BUCKET(n,i) (&(*((Entry **)(n) + i)))

/**
 * Map a hash value to the bucket corresponding to the hash value.  
 */ 
HashIndex::Entry **HashIndex::getBucket (Hash hashValue) const
{	
	char *bucketIndexNode;
		
	for (unsigned int l = numLayers - 1; l > 0  ; 
		 hashValue >>= numBitsPerLayer, l--)	
		pathToBucket [l] = NON_ROOT_MASK & hashValue; 
	
	pathToBucket [0] = ROOT_MASK & hashValue;
	
	
	bucketIndexNode = bucketIndexRoot;
	for (unsigned int l = 0 ; l + 1  < numLayers ; l++) {
		bucketIndexNode = CHILD (bucketIndexNode, pathToBucket [l]);
	}
	
	return BUCKET(bucketIndexNode, pathToBucket [numLayers - 1]);
}

#define NEXT_PAGE(p) (*((char **)(p)))

#define DATA_PART(p) ((Entry*)((p) + dataPartOffset))

/**
 * Get space for a new hash entry:
 */
HashIndex::Entry *HashIndex::getNewEntry () 
{
	int rc;
	Entry        *retEntry;
	char         *pagePtr;
	Entry        *freeEntry;
	
	// We do not have any space with us:
	if (!freeEntriesList) {
		
		// Allocate a new page.
		if ((rc = memMgr -> allocatePage (pagePtr)) != 0) {
			
			LOG << "HashIndex [" << id << "]: "
				<< "Unable to get pages from memory manager" 
				<< endl;
			
			ASSERT (0);
			return 0;
		}
		
		// Maintain the linked list of pages
		NEXT_PAGE (pagePtr) = dataPageList;
		dataPageList = pagePtr;
		
		// Assign all the available space on this page to empty entries
		// list 
		freeEntriesList = freeEntry = DATA_PART (pagePtr);
		
		// Note: we are only doing (numEntriesPerPage - 1) iterations
		for (unsigned int e = 1 ; e < numEntriesPerPage ; e++) {
			freeEntry -> next = freeEntry + 1;
			freeEntry ++;
		}
		freeEntry -> next = 0;
	}
	
	retEntry = freeEntriesList;
	freeEntriesList = freeEntriesList -> next;
	
	return retEntry;
}

/**
 * Return a freed up entry to the free entries list
 */ 
void HashIndex::freeEntry (Entry *entry)
{
	entry -> next = freeEntriesList;
	freeEntriesList = entry;
	
	return;
}

int HashIndex::insertTuple (Tuple tuple)
{
	Hash     hashValue;
	Entry  **bucket;
	Entry   *newEntry;

#ifdef _MONITOR_
	numEntries++;
#endif
	
	// Compute the hash of the tuple
	evalContext -> bind (tuple, UPDATE_ROLE);
	hashValue = updateHashEval -> eval();
	
	// Get the bucket (linked list of hash entries that hash to this bucket)
	bucket = getBucket (hashValue);	
	
	// We are inserting the first entry in this bucket
	if (! (*bucket)) 
		numNonEmptyBuckets ++;
	
	// Add the new entry to the bucket
	newEntry = getNewEntry ();	
	newEntry -> dataPtr = tuple;
	newEntry -> next = *bucket;	
	*bucket = newEntry;		
	
	ASSERT (numNonEmptyBuckets <= numBuckets);
	if (numNonEmptyBuckets * 1.0 / numBuckets > threshold)
		doubleNumBuckets ();
	
	return 0;
}

int HashIndex::deleteTuple (Tuple tuple)
{
	Hash     hashValue;
	Entry  **entryList, **bucket;
	Entry   *delEntry;

#ifdef _MONITOR_
	numEntries++;
#endif
	
	// compute the hash of the tuple
	evalContext -> bind (tuple, UPDATE_ROLE);
	hashValue = updateHashEval -> eval();
	
	// get the bucket for this hash value
	bucket = entryList = getBucket (hashValue);
	
	// Iterate over the entries in the bucket
	for ( ; (*entryList) && ( (*entryList) -> dataPtr != tuple) ; 
		  entryList = &((*entryList)->next))
		;
	
	// We found the tuple
	if (*entryList) {		
		delEntry = *entryList;
		*entryList = delEntry -> next;		
		freeEntry (delEntry);
		
		// The bucket became empty after this deletion.
		if (! (*bucket))
			numNonEmptyBuckets --;
		
		return 0;
	}
	
	// We failed to find this tuple:
	return -1;	
}

int HashIndex::getScan (TupleIterator *& _iter)
{
	int rc;
	Hash      hashValue;
	Entry   **bucket;
	
	// Compute the hash 
	hashValue = scanHashEval -> eval();
	
	// The bucket to be scanned.
	bucket = getBucket (hashValue);

	// Initialize my iterator
	if ((rc = iter -> initialize (*bucket)) != 0)
		return rc;
	
	_iter = iter;
	
	// Iterator iterates over this bucket ..
	return 0;
}

int HashIndex::releaseScan (TupleIterator *iter)
{   
	return 0;
}

int HashIndex::doubleNumBuckets () 
{
	int rc;
	Hash          newBit;
	unsigned int  numBitsAtRoot;
	
	// We will log the fact that we are doubling
	LOG << "HashIndex [" << id << "]: " 
		<< "Doubling number of buckets to " << numBuckets * 2 
		<< endl;
	
	// The new bit position that will be now be used for hashing to
	// buckets 
	newBit = (1 << numBits);
	
	// We'll be recomputing this from scratch ...
	numNonEmptyBuckets = 0;

	// Note: numBitsAtRoot is meaningful only if the "if" condition
	// fails.  If the condition succeeds, then we have to add a new
	// root to the bucket index, otherwise, we need not have to.
	if ((numBitsAtRoot = numBits % numBitsPerLayer) == 0) {
		char         *newRoot, *oldRootsSibling;	
		
		// Allocate a page for the new root
		if((rc = memMgr -> allocatePage (newRoot)) != 0) {
			
			LOG << "HashIndex [" << id << "]: "
				<< "Unable to allocate page" << endl;
			
			ASSERT (0);
			return rc;
		}
		
		// Allocate a page for the old root's sibling
		if((rc = memMgr -> allocatePage (oldRootsSibling)) != 0) {
			
			LOG << "HashIndex [" << id << "]: "
				<< "Unable to allocate page" << endl;
			
			ASSERT (0);
			return rc;
		}
		
		// 2 children of the new root:
		CHILD(newRoot,0) = bucketIndexRoot;
		CHILD(newRoot,1) = oldRootsSibling;
		
		// Create new buckets under oldRootsSibling node, and rehash the
		// index entries from the old root to buckets under old roots
		// sibling node.
		if ((rc = rehashBucketIndexNode (bucketIndexRoot, 
										 oldRootsSibling,
										 numLayers - 1,
										 newBit)) != 0) {
			return rc;
		}
		
		ROOT_MASK = 1;
		numLayers ++;		
		bucketIndexRoot = newRoot; 
	}
	
	// The old root remains
	else {		
		unsigned int  numChildrenOfRoot;
		char         *newChild;
		
		// Number of children indexed @ the root node
		numChildrenOfRoot = 1 << numBitsAtRoot;
		
		ASSERT ((numChildrenOfRoot * 2) <= (pageSize / sizeof(void *)));
		
		// [[ Explanation ]]
		ASSERT (numLayers > 1);
		
		for (unsigned int c = 0 ; c < numChildrenOfRoot ; c++) {
			
			// create a new child:
			if ((rc = memMgr -> allocatePage (newChild)) != 0) {
				LOG << "HashIndex [" << id << "]: "
					<< "Unable to allocate page" << endl;
				
				return rc;
			}
			
			CHILD (bucketIndexRoot, c + numChildrenOfRoot) = newChild;
			
			rc = rehashBucketIndexNode (CHILD (bucketIndexRoot,c),
										newChild, 
										numLayers - 2,
										newBit);
			if (rc != 0)
				return rc;
		} 
		
		ROOT_MASK |= (ROOT_MASK + 1);
	}
	
	numBits ++;
	numBuckets *= 2;
		
	return 0;
}

int HashIndex::rehashBucketIndexNode (char *oldNode, char *newNode, 
									  unsigned int height, Hash newBit) 
{	
	int rc;

	// Nonleaf node: [[ This should occur only for laaaarge indexes ]]
	if (height > 0) {
		unsigned int  numChildNodes;
		char         *newChild;
		
		numChildNodes = (1 << numBitsPerLayer);
		
		for (unsigned int c = 0 ; c < numChildNodes ; c++) {
			
			if ((rc = memMgr -> allocatePage (newChild)) != 0) {
				
				LOG << "HashIndex [" << id << "]: "
					<< "Unable to allocate page" << endl;
				
				return rc;
			}
			
			CHILD (newNode,c) = newChild;
			
			// recursively rehash the children
			if ((rc = rehashBucketIndexNode (CHILD(oldNode, c),
											 newChild,
											 height - 1, newBit)) != 0)
				return rc;			
		}
	}
	
	// Leaf node
	else {
		unsigned int numBuckets;
		
		numBuckets = (1 << numBitsPerLayer);
		
		for (unsigned b = 0 ; b < numBuckets ; b++) {
			
			// rehash entries from the old bucket to the new bucket.
			if ((rc = rehashBucket (BUCKET(oldNode,b),
									BUCKET(newNode,b),
									newBit)) != 0)
				return rc;
		}
	}
	
	return 0;
}
	
int HashIndex::rehashBucket (Entry **oldBucket, Entry **newBucket, 
							 Hash newBit)
{
	int bOldBucketNonEmpty = 0;
	int bNewBucketNonEmpty = 0;

	while (*oldBucket) {
		evalContext -> bind ((*oldBucket)->dataPtr, UPDATE_ROLE);
		
		// Rehashes to the new bucket
		if ((updateHashEval -> eval()) & newBit) {
			*newBucket = *oldBucket;
			newBucket = &((*newBucket) -> next);
			*oldBucket = *newBucket;
			bNewBucketNonEmpty = 1;
		}
		
		else {
			oldBucket = &((*oldBucket) -> next);
			bOldBucketNonEmpty = 1;
		}
	}
	
	*newBucket = 0;
	
	numNonEmptyBuckets += (bOldBucketNonEmpty + bNewBucketNonEmpty);	
	return 0;
}

void HashIndex::printDist () const
{
	Entry **bucket;
	Entry *entry;
	unsigned int count;
	
	for (Hash h = 0 ; h < numBuckets ; h++) {
		bucket = getBucket (h);	
		
		count = 0;
		entry = *bucket;
		
		while (entry) {
			count++;
			entry = entry -> next;
		}
		
		LOG << count << endl;
	}
}

#ifdef _MONITOR_
#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

int HashIndex::getIntProperty (int property, int& val)
{
	if (property == Monitor::HINDEX_NUM_BUCKETS) {
		val = (int)numBuckets;
		return 0;
	}
	
	if (property == Monitor::HINDEX_NUM_NONMT_BUCKETS) {
		val = (int)numNonEmptyBuckets;
		return 0;
	}
	
	if (property == Monitor::HINDEX_NUM_ENTRIES) {
		val = numEntries;
		return 0;
	}	
	
	return PropertyMonitor::getIntProperty (property, val);
}
#endif
