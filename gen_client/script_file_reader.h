#ifndef _SCRIPT_FILE_READER_
#define _SCRIPT_FILE_READER_

#include <fstream>

namespace Client {
	
	/**
	 * Reader for STREAM scripts.  Reads a script file, does some very
	 * preliminary interpretation - for example, separating out different
	 * kinds of commands - and returns the STREAM system commands one after
	 * another.
	 */
	class ScriptFileReader {
	public:
		
		/**
		 * Different types of commands:
		 */
		enum Type {
			TABLE,    ///< (Base) table (stream/relation) specification
			SOURCE,   ///< source info for a table
			QUERY,    ///< Continuous query with output
			DEST,     ///< Destn info a query
			VQUERY,   ///< Query part of a view
			VTABLE    ///< Schema part of a view
		};
		
		/**
		 * Representation of a (partially) parsed line containing a
		 * command.		
		 */		
		struct Command {
			/// Line no in the script file where this command occurs
			unsigned int      lineNo;   
			
			/// Type of command
			Type              type;     
			
			/// Encoded command description (understood by the parser)
			const char       *desc;
			
			// Length of the command description string
			unsigned int      len;			
		};
		
		/**
		 *  @param    fileName      name of the script file
		 */
		ScriptFileReader (const char *fileName);
		
		~ScriptFileReader();
		
		/**
		 * Get the next line containing the command.
		 */
		int getNextCommand(Command& command);
		
	private:
		
		/// Input file stream for the script file
		std::ifstream inputFile;
		
		/// Number of lines of the script file processed so far.
		unsigned int lineNo;		
		
		/// Buffer for storing command descriptions.
		static const unsigned int LINE_BUFFER_SIZE = 8192;
		char lineBuffer [LINE_BUFFER_SIZE];
		
		//----------------------------------------------------------------------
		// Helper functions
		//----------------------------------------------------------------------
		
		/**
		 * Determine is a line is "empty" -- just contains whitespace
		 */
		bool isEmptyLine (const char *line) const;
		
		/**
		 * Determine if a line is comment line --- starts with '#'
		 */
		bool isCommentLine (const char *line) const;
		
		/**
		 * Parse a line: [[ Explanation ]]
		 */
		int parseLine (char           *lineBuffer,					   
					   Command        &command);
	};
}
#endif
