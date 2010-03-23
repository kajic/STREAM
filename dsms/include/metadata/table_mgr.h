#ifndef _TABLE_MGR_
#define _TABLE_MGR_

/**
 * @file     tablemgr.h
 * @date     May 20, 2004
 * @brief    The table manager that contains metadata about the registered
 *           streams and relations (tables) in the system.
 */

#include "common/types.h"
#include "common/constants.h"

namespace Metadata {
  
	/**
	 * Manager for named streams and relations (input and intermediate) in the
	 * system.
	 */
	
	class TableManager {
	private:
		
		// Attribute of a table
		struct Attribute {
			char         *attrName;
			Type          attrType;
			unsigned int  attrLen;			
		};
		
		// Stream / relation
		struct Table {
			char           *tableName;
			bool            isStream;
			unsigned int    numAttrs;
			Attribute       attrList [ MAX_ATTRS ];
		};		
		
		// Number of tables currently registered
		unsigned int        numTables;
		
		// Information about currently registered tables
		Table               tableList [ MAX_TABLES ];
		
	public:
		TableManager();		
		~TableManager();
		
		//----------------------------------------------------------------------
		//
		// Methods to register/construct streams and relations
		//
		//----------------------------------------------------------------------
		
		/**
		 *
		 * Register a new stream in  the system.  Each stream is identified in
		 * the internally in the system  by a unique integer identifier, which
		 * is returned by this function call.
		 * 
		 * @param streamName     name of the stream to be registered.
		 * @param streamId       returned internal identifier for the stream.
		 * @return               0 (success) !0 (failure)   
		 */                       
    
		int registerStream(const char     *streamName, 
						   unsigned int&   streamId);
    
    
		/**
		 * 
		 * Register a new relation in the system and return the unique
		 * identifer to the relation.
		 *
		 * @param relName       name of the relation.
		 * @param relId         returned identifier for the relation
		 * @return              0 (success) !0 (failure)
		 */
		int registerRelation(const char       *relName,
							 unsigned int&     relId);
		
		int unregisterTable (unsigned int tableId);
		
		/**
		 * 
		 * Add an attribute to an already registered stream / relation
		 * 
		 * @param tableId        Identifier for the stream
		 *                       (returned by registerStream)
		 * @param attrName       Name of the attribute
		 * @param attrType       Data Type of attribute
		 * @param attrLen        Length of the attribute: useful for strings.
		 *
		 * @return               0 (success), !0 (failure)
		 *
		 */
		
		int addTableAttribute(unsigned int      tableId,
							  const char       *attrName,
							  Type              attrType,
							  unsigned int      attrLen);
		
		//---------------------------------------------------------------------
		//
		// Lookup methods:
		//
		//---------------------------------------------------------------------
		
		/**
		 * 
		 * Get the identifier of a registered stream / relation.
		 *
		 * @param  tableName    name of the stream/relation.
		 * @param  tableId      returned id of the stream if found
		 * @return              true if tableName found, false otherwise.
		 *
		 */
    
		bool getTableId(const char *tableName, unsigned int& tableId) const;
		
		/**
		 * Get the number of attributes in a table (stream/ relation)
		 *
		 * @param the number of attributes, 0 on error.
		 */
		unsigned int getNumAttrs(unsigned int tableId) const;
		
		/**
		 * Get the identifer of an attribute.  An identifier of an attribute
		 * is its position within the attributes of the stream - so 
		 * it is not globally unique, but <tableId, attrId> is unique
		 * 
		 * @param  tableId     identifier of the stream/relation.
		 * @param  attrName    name of the attribute looked up.
		 * @param  attrId      returned identifier of the attribute.
		 * @return             true if atribute found, false otherwise.
		 */
    
		bool getAttrId(unsigned int  tableId, const char *attrName,		   
					   unsigned int& attrId) const;
    
		/**
		 * Get the type of a specified stream/relation attribute.
		 * 
		 * @param relStrId    identifier of the stream/relation
		 * @param attrId      identifier of the relation
		 * @return            the type of the attribute
		 */
    
		Type getAttrType(unsigned int  relStrId, unsigned int  attrId) const;		     
    
		/**
		 * Get the length of a specified stream/relation attribute
		 *
		 * @param relStrId    identifier for a relation/stream
		 * @param attrId      identifer for an attribute
		 * @return            the type of the attribute
		 */    
    
		unsigned int getAttrLen(unsigned int relStrId, unsigned int attrId)
			const;
		
		/**
		 * Determine if a table is a stream or not
		 *
		 * @param    tableId  identifier of the table
		 * @return            true if table is a stream, false otherwise.
		 */ 
		bool isStream(unsigned int tableId) const;   

		/**
		 * Get the table name given the table Id
		 */		
		const char *getTableName (unsigned int tableId) const;
		
		/// Debug
		void printState() const;

#ifdef _SYS_STR_
		void registerSysStr ();
#endif
	};
}

#endif
