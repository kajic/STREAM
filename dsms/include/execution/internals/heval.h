#ifndef _HEVAL_
#define _HEVAL_

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#define CHECK(b) { if(!(b)) return false;}
#define ILOC(c,o) (*(((int *)roles[(c)]) + (o)))
#define FLOC(c,o) (*(((float *)roles[(c)]) + (o)))
#define BLOC(c,o) (*((roles[(c)]) + (o)))
#define CLOC(c,o) ((roles[(c)]) + (o))

namespace Execution {
	
	struct HInstr {
		Type         type;
		unsigned int r;
		Column       c;
	};
	
	class HEval {
	private:
		static const unsigned int MAX_INSTRS = 20;
		char         **roles;
		unsigned int   numInstrs;
		HInstr         instrs [MAX_INSTRS];
		
	public:
		HEval ();
		~HEval ();
		
		int setEvalContext (EvalContext *evalContext);			
		int addInstr (HInstr instr);
		
		inline Hash eval () const {			
			Hash hash = 5381;			
			char *cptr; 
			int c;
			
			for (unsigned int i = 0 ; i < numInstrs ; i++) {
				switch (instrs[i].type) {
				case INT:
					hash = ((hash << 5) + hash) + 
						inthash (ILOC(instrs[i].r, instrs[i].c));
					break;
					
				case BYTE:
					
					hash = ((hash << 5) + hash) + 
						BLOC(instrs[i].r, instrs[i].c);
					break;
					
				case CHAR:
					
					cptr = CLOC (instrs[i].r, instrs[i].c);
					while ((c = *cptr++))
						hash = ((hash << 5) + hash) + c;
					break;
					
				case FLOAT:
					break;
					
				default:					
					break;
				}
			}
			
			return hash;
		}

	private:
		
		// Hash function from 
		// http://www.concentric.net/~Ttwang/tech/inthash.htm
		inline int inthash(int key) const {			
			key += ~(key << 15);
			key ^=  (key >> 10);
			key +=  (key << 3);
			key ^=  (key >> 6);
			key += ~(key << 11);
			key ^=  (key >> 16);
			return key;
		}
		
		//inline int inthash(int key) const {
		//	return key;
		//}		
	};
}

#endif
